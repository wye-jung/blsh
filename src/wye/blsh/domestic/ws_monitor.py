"""
실시간 체결가 웹소켓 모니터
─────────────────────────────────────────────────────
KIS WebSocket API(ccnl_total, H0UNCNT0)로 KRX+NXT 통합 실시간 체결가 수신.
백그라운드 데몬 스레드에서 asyncio 이벤트 루프 실행.

설계 원칙 (KIS 무한연결 차단 방지):
    - 세션당 단일 WebSocket 연결 유지 (연결/종료 반복 금지)
    - 재연결 지수 백오프 (5초→10초→20초→...→60초), 성공 시 리셋
    - 정상 흐름: 연결 → 구독 → 수신 → 불필요 해제 → 연결 종료
    - 구독 변경은 포지션 실제 변동 시에만 (무의미 등록/해제 반복 금지)
    - 실패 시 REST 폴링으로 자동 fallback (트레이더 중단 없음)

    MAX 40종목: 초과분은 REST 폴링
    USE_WEBSOCKET=0: 전체 REST 폴링 (기존 방식)
"""

import asyncio
import json
import logging
import queue
import threading
import time

from wye.blsh.kis import kis_auth as ka
from wye.blsh.kis.domestic_stock import domestic_stock_functions_ws as ws_fn

log = logging.getLogger(__name__)

MAX_WS_SUBSCRIPTIONS = 40  # KIS 세션당 최대 구독 수
_RECONNECT_MAX = 5
_RECONNECT_BASE_DELAY = 5  # 초 (지수 백오프: 5→10→20→40→60)
_RECONNECT_MAX_DELAY = 60
_CMD_BATCH_DELAY = 0.3  # 구독 변경 간 대기 (초)
_STALE_THRESHOLD_SEC = 30.0   # WS 캐시 가격 신선도 임계치 (trader TICK_SEC=10초 × 3)
_RECONNECT_GRACE_SEC = 5.0    # 재연결 직후 WS 캐시 우회 기간
_FALLBACK_LOG_INTERVAL = 30.0  # REST fallback info 로그 간 최소 간격


class PriceMonitor:
    """실시간 체결가 웹소켓 + REST 하이브리드 모니터.

    use_ws=True:  보유종목(최대 40개)을 웹소켓으로 실시간 모니터링.
                  초과분은 REST 폴링으로 자동 fallback.
    use_ws=False: 전체 종목 REST 폴링 (기존 방식).
    """

    def __init__(self, kis_client, use_ws: bool = True):
        self._kis = kis_client  # KISClient 인스턴스 (REST fallback용)
        self._use_ws = use_ws

        # thread-safe 가격 캐시: ticker → (price, monotonic_ts)
        # ts 동반 저장으로 get_prices에서 freshness 검증 → stale 시 REST fallback
        self._prices: dict[str, tuple[float, float]] = {}
        self._lock = threading.Lock()

        # 구독 상태
        self._subscribed: set[str] = set()

        # 재연결 직후 WS 캐시 우회용 타임스탬프 (grace window)
        self._reconnect_ts: float | None = None

        # REST fallback 로그 도배 방지
        self._last_fallback_log_ts: float = 0.0

        # 메인→WS 스레드 커맨드 큐
        self._cmd_queue: queue.Queue = queue.Queue()

        # 상태
        self._running = False
        self._connected = False
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

    # ─────────────────────────────────────
    # 공개 API (메인 스레드에서 호출)
    # ─────────────────────────────────────
    @property
    def ws_active(self) -> bool:
        """웹소켓 연결 활성 여부."""
        with self._lock:
            return self._connected and self._running

    @property
    def ws_ticker_count(self) -> int:
        """현재 WS 구독 중인 종목 수."""
        with self._lock:
            return len(self._subscribed)

    def start(self, tickers: list[str] | None = None):
        """웹소켓 백그라운드 스레드 시작. tickers: 초기 구독 종목."""
        if not self._use_ws or self._running:
            return

        # WebSocket 인증 (메인 스레드에서 실행 — 1회)
        ws_svr = "prod" if self._kis.env_dv == "real" else "vps"
        try:
            ka.auth_ws(ws_svr)
        except Exception as e:
            log.warning(f"[WS] auth_ws 실패: {e} — REST 폴링으로 계속")
            self._use_ws = False
            return

        # 초기 구독 종목 등록
        initial = list(tickers or [])[:MAX_WS_SUBSCRIPTIONS]
        for t in initial:
            self._cmd_queue.put(("add", t))

        self._running = True
        self._thread = threading.Thread(
            target=self._run_loop, daemon=True, name="ws-price-monitor"
        )
        self._thread.start()
        log.info(
            f"[WS] PriceMonitor 시작 (초기 {len(initial)}종목, 최대 {MAX_WS_SUBSCRIPTIONS})"
        )

    def stop(self):
        """WebSocket 연결 종료 (세션 끝에 1회 호출).

        "stop" 커맨드를 큐에 넣고 WS 스레드가 구독 해제 후 종료하도록 대기.
        _running을 먼저 False로 하지 않음 → _process_commands가 unsubscribe 전송 후 False 설정.
        """
        if not self._running:
            return
        # _running 유지 → _receive_loop가 "stop" 커맨드를 처리할 기회 보장
        self._cmd_queue.put(("stop", ""))
        if self._thread:
            self._thread.join(timeout=10)
            self._thread = None
        # 스레드 종료 후 정리
        self._running = False
        with self._lock:
            self._subscribed.clear()
            self._prices.clear()
        log.info("[WS] PriceMonitor 종료")

    def get_prices(self, tickers: list[str]) -> dict[str, float]:
        """종목별 현재가 조회 (WS 가격 + REST fallback).

        WS 구독 중이고 신선한(≤ _STALE_THRESHOLD_SEC) 가격만 WS 캐시에서 반환.
        미구독 / 미수신 / stale / 재연결 grace window 내 → REST 폴링.
        use_ws=False이면 전체 REST 폴링.
        """
        if not self._use_ws or not self._running:
            return self._kis.fetch_prices(tickers)

        result: dict[str, float] = {}
        rest_needed: list[str] = []
        now = time.monotonic()

        with self._lock:
            in_grace = (
                self._reconnect_ts is not None
                and now - self._reconnect_ts < _RECONNECT_GRACE_SEC
            )
            for t in tickers:
                if (
                    not in_grace
                    and t in self._subscribed
                    and t in self._prices
                ):
                    price, ts = self._prices[t]
                    if now - ts <= _STALE_THRESHOLD_SEC:
                        result[t] = price
                        continue
                rest_needed.append(t)

        if rest_needed:
            rest_prices = self._kis.fetch_prices(rest_needed)
            result.update(rest_prices)
            if (
                (in_grace or len(rest_needed) >= max(1, len(tickers) // 2))
                and now - self._last_fallback_log_ts >= _FALLBACK_LOG_INTERVAL
            ):
                log.info(
                    f"[WS] REST fallback {len(rest_needed)}/{len(tickers)}종목"
                    f" (grace={in_grace})"
                )
                self._last_fallback_log_ts = now

        return result

    def sync_subscriptions(self, tickers: list[str]):
        """보유종목 변경 시 구독 동기화 (포지션 변동 시에만 호출).

        변경 없으면 큐 오염 방지를 위해 아무것도 하지 않음.
        """
        if not self._use_ws or not self._running:
            return

        wanted = set(tickers[:MAX_WS_SUBSCRIPTIONS])
        with self._lock:
            current = set(self._subscribed)

        if wanted == current:
            return  # 변경 없음

        to_remove = current - wanted
        to_add = wanted - current

        for t in to_remove:
            self._cmd_queue.put(("remove", t))

        available = MAX_WS_SUBSCRIPTIONS - (len(current) - len(to_remove))
        for t in sorted(to_add)[: max(0, available)]:
            self._cmd_queue.put(("add", t))

    # ─────────────────────────────────────
    # 백그라운드 스레드 (asyncio 이벤트 루프)
    # ─────────────────────────────────────
    def _run_loop(self):
        """백그라운드 데몬 스레드 진입점."""
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._ws_main())
        except Exception as e:
            if self._running:
                log.warning(f"[WS] 이벤트 루프 예외: {e}")
        finally:
            self._loop.close()
            self._loop = None
            self._connected = False
            self._running = False

    async def _ws_main(self):
        """WebSocket 연결 + 지수 백오프 재연결 루프.

        연속 _RECONNECT_MAX회 실패 시 REST fallback으로 전환.
        성공 연결 후 다시 끊기면 실패 카운터 리셋.
        """
        import websockets

        url = f"{ka.getTREnv().my_url_ws}/tryitout"
        backoff = _RECONNECT_BASE_DELAY
        consecutive_failures = 0

        while self._running:
            try:
                async with websockets.connect(url, ping_interval=None) as ws:
                    self._connected = True
                    backoff = _RECONNECT_BASE_DELAY  # 성공 → 백오프 리셋
                    consecutive_failures = 0  # 성공 → 실패 카운터 리셋
                    log.info(f"[WS] 연결 성공")

                    # 재연결 시 stale 가격 캐시 클리어 + grace window 설정
                    # → 재구독 후 첫 틱 수신 전까지 get_prices가 REST fallback으로
                    # 신선한 가격 사용 (트레일링 SL high 누락 방지)
                    with self._lock:
                        self._prices.clear()
                        self._reconnect_ts = time.monotonic()
                        restore_list = list(self._subscribed)
                    for ticker in restore_list:
                        await self._send_subscribe(ws, ticker)
                    if restore_list:
                        log.info(f"[WS] {len(restore_list)}종목 재구독 완료 (가격 캐시 클리어)")

                    # 수신 루프 (+ 커맨드 처리)
                    await self._receive_loop(ws)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self._connected = False
                consecutive_failures += 1
                if not self._running:
                    break
                if consecutive_failures >= _RECONNECT_MAX:
                    log.warning(f"[WS] {_RECONNECT_MAX}회 연속 실패 → REST 폴링 전환")
                    break
                log.warning(
                    f"[WS] 연결 끊김 ({consecutive_failures}/{_RECONNECT_MAX}): {e}"
                    f" — {backoff}초 후 재연결"
                )
                for _ in range(int(backoff)):
                    if not self._running:
                        break
                    await asyncio.sleep(1)
                backoff = min(backoff * 2, _RECONNECT_MAX_DELAY)

        self._connected = False

    async def _receive_loop(self, ws):
        """메시지 수신 + 커맨드 처리 루프."""
        while self._running:
            # 큐의 구독 변경 명령 처리
            await self._process_commands(ws)

            # WebSocket 메시지 수신 (1초 타임아웃 → 커맨드 재확인)
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except Exception:
                if self._running:
                    raise  # 재연결 트리거
                return

            await self._handle_message(raw, ws)

    async def _process_commands(self, ws):
        """큐의 add/remove 명령 일괄 처리."""
        processed = 0
        while not self._cmd_queue.empty():
            try:
                action, ticker = self._cmd_queue.get_nowait()
            except queue.Empty:
                break

            if action == "stop":
                # 정상 종료: 모든 구독 해제 후 종료
                with self._lock:
                    to_unsub = list(self._subscribed)
                for t in to_unsub:
                    await self._send_unsubscribe(ws, t)
                with self._lock:
                    self._subscribed.clear()
                self._running = False
                return

            if action == "add":
                with self._lock:
                    already = ticker in self._subscribed
                    full = len(self._subscribed) >= MAX_WS_SUBSCRIPTIONS
                if already:
                    continue
                if full:
                    log.debug(f"[WS] 구독 한도 초과, 무시: {ticker}")
                    continue
                await self._send_subscribe(ws, ticker)
                with self._lock:
                    self._subscribed.add(ticker)
                    count = len(self._subscribed)
                processed += 1
                log.info(f"[WS] 구독 추가: {ticker}  ({count}/{MAX_WS_SUBSCRIPTIONS})")

            elif action == "remove":
                with self._lock:
                    exists = ticker in self._subscribed
                if not exists:
                    continue
                await self._send_unsubscribe(ws, ticker)
                with self._lock:
                    self._subscribed.discard(ticker)
                    self._prices.pop(ticker, None)
                    count = len(self._subscribed)
                processed += 1
                log.info(f"[WS] 구독 해제: {ticker}  ({count}/{MAX_WS_SUBSCRIPTIONS})")

            # 구독 변경 간 대기 (KIS 서버 부하 방지)
            if processed > 0 and not self._cmd_queue.empty():
                await asyncio.sleep(_CMD_BATCH_DELAY)

    async def _send_subscribe(self, ws, ticker: str):
        """종목 구독 등록."""
        msg, columns = ws_fn.ccnl_total("1", ticker)
        tr_id = msg["body"]["input"]["tr_id"]
        ka.add_data_map(tr_id=tr_id, columns=columns)
        await ws.send(json.dumps(msg))
        await asyncio.sleep(0.1)  # KIS rate limit

    async def _send_unsubscribe(self, ws, ticker: str):
        """종목 구독 해제."""
        msg, _ = ws_fn.ccnl_total("2", ticker)
        await ws.send(json.dumps(msg))
        await asyncio.sleep(0.1)

    async def _handle_message(self, raw: str, ws):
        """수신 메시지 파싱 → 가격 업데이트 / 시스템 응답 처리."""
        if not raw:
            return

        if raw[0] in ("0", "1"):
            # 실시간 데이터: flag|tr_id|count|data
            parts = raw.split("|")
            if len(parts) < 4:
                return

            tr_id = parts[1]
            dm = ka.data_map.get(tr_id)
            if not dm:
                return

            data = parts[3]
            if dm.get("encrypt") == "Y":
                try:
                    data = ka.aes_cbc_base64_dec(dm["key"], dm["iv"], data)
                except Exception:
                    return

            # MKSC_SHRN_ISCD^STCK_CNTG_HOUR^STCK_PRPR^...
            fields = data.split("^")
            if len(fields) >= 3:
                ticker = fields[0]
                try:
                    price = float(fields[2])  # STCK_PRPR
                    if price > 0:
                        now = time.monotonic()
                        with self._lock:
                            self._prices[ticker] = (price, now)
                except (ValueError, IndexError):
                    pass
        else:
            # 시스템 메시지 (JSON)
            try:
                rsp = ka.system_resp(raw)
                ka.add_data_map(
                    tr_id=rsp.tr_id,
                    encrypt=rsp.encrypt,
                    key=rsp.ekey,
                    iv=rsp.iv,
                )
                if rsp.isPingPong:
                    await ws.pong(raw)
            except Exception as e:
                log.debug(f"[WS] 시스템 메시지 파싱 실패: {e}")
