"""
자동 매매 트레이더 v2
─────────────────────────────────────────────────────
실행:
    uv run python -m blsh.wye.domestic.trader3

환경변수:
    KIS_ENV=demo   모의투자 (기본)
    KIS_ENV=real   실전투자 🚨

투자 전략:
    1. 09:00 포지션 읽어와서 보유종목 모니터링.
        장 중 30초 간격 전 종목 현재가 병렬 조회 → ATR 기반 SL/TP 처리
        - 손절: 현재가 ≤ dynamic_sl → 전량 시장가 매도
        - 1차 익절(TP1 = buy+ATR×1.0): 50% 매도, SL → 매수가(본전 보장)
        - 2차 익절(TP2 = buy+ATR×ATR_TP_MULT): 잔여 전량 매도
        - 트레일링 SL: 주가 상승 시 SL을 (현재가 - ATR×ATR_SL_MULT) 로 상향
    2. 모니터링 중 ~/.blsh/data/po 폴더에 po_*.json 파일(po_after_liquidate.json 제외)이 존재하면 읽어들여 지정가 매수.
        매도가 지연되지 않게 쓰레드 처리 고려.
        after_liquidate가 "Y" 인 종목은 15:20 청산 이후 매수
        기 보유종목은 매수 제외.
        10분 후 미체결 주문 취소.
        읽어들여 처리한 json파일은 ~/.blsh/data/po/done 폴더로 이동.
    3. 15:20 청산
        청산 조건
        - 오늘이 청산일인 종목. 청산하지 못하면 다음 영업일 재실행 시 청산.
          포지션은 ~/.blsh/data/positions.json 에 영속 저장
    4. 청산 직후 ~/.blsh/data/po 폴더에 po_after_liquidate.json 제외이 존재하면 읽어들여 지정가 매수.
        기 보유종목은 매수 제외.
        잔고의 1/2 만 사용.
    비고 : 매수, 매도 성공시 ~/.blsh/data/history/history_오늘날짜.json 저장.

데이/스윙 모드: 종목 모드별 _factor.py MAX_HOLD_DAYS 기준으로 자동 분류
    MOM → MAX_HOLD_DAYS_MOM, MIX → MAX_HOLD_DAYS_MIX, REV → MAX_HOLD_DAYS
    max_hold_days == 0 이면 데이 트레이딩
─────────────────────────────────────────────────────
"""

import asyncio
import json
import logging
import os
import threading
import time

from dataclasses import asdict, dataclass
from pathlib import Path

from sqlalchemy import text

from blsh.wye.domestic import _factor as fac, collector, scanner
from blsh.wye.domestic._kis import _Api
from blsh.database import engine, query
from blsh.common import dtutils
from blsh.wye.domestic._tick import floor_tick as _floor_tick, ceil_tick as _ceil_tick

log = logging.getLogger(__name__)

# ─────────────────────────────────────────
# 설정
# ─────────────────────────────────────────
CASH_USAGE = 0.9            # 가용 현금 비율
AFTERLIQ_CASH_RATIO = 0.5   # 청산 후 매수에 사용할 잔고 비율 (1/2)
MIN_ALLOC = 10_000          # 종목당 최소 배분액

SCAN_MORNING = "100000"     # 10:00 오전 스캔+매수 시작
CANCEL_MORNING = "101000"   # 10:10 미체결 취소
SCAN_CLOSING = "152000"     # 15:20 오후 스캔+청산+매수 시작
MARKET_CLOSE = "153000"     # 장 마감

TP1_MULT = 1.0
SELL_COST_RATE = 0.002      # 증권거래세 + 수수료 합산
POLL_SEC = 30
GAP_DOWN_LIMIT = 0.03       # 갭하락 하한: entry 대비 3% 이상 하락 시 스킵

POSITIONS_FILE = Path.home() / ".blsh" / "data" / "positions.json"


# ─────────────────────────────────────────
# 포지션
# ─────────────────────────────────────────
@dataclass
class Position:
    ticker: str
    name: str
    qty: int            # 현재 잔여 수량
    buy_price: float    # 실제 체결가
    atr: float          # 진입 시 ATR
    atr_sl_mult: float  # 스캔 시점 ATR_SL_MULT
    atr_tp_mult: float  # 스캔 시점 ATR_TP_MULT
    sl: float           # 현재 동적 손절가 (트레일링)
    tp1: float          # 1차 목표가 (buy + ATR×TP1_MULT)
    tp2: float          # 2차 목표가 (buy + ATR×atr_tp_mult)
    mode: str
    max_hold_days: int
    entry_date: str     # YYYYMMDD
    expiry_date: str = ""
    t1_done: bool = False
    qty_t1: int = 0
    realized_pnl: float = 0.0


def _make_position(
    c: dict, buy_price: float, qty: int, entry_date: str, expiry_date: str = ""
) -> Position:
    atr = float(c["atr"])
    atr_sl_mult = float(
        c["atr_sl_mult"] if c.get("atr_sl_mult") is not None else fac.ATR_SL_MULT
    )
    atr_tp_mult = float(
        c["atr_tp_mult"] if c.get("atr_tp_mult") is not None else fac.ATR_TP_MULT
    )
    max_hold = int(
        c["max_hold_days"] if c.get("max_hold_days") is not None else fac.MAX_HOLD_DAYS
    )
    sl = _floor_tick(buy_price - atr_sl_mult * atr)
    tp1 = _ceil_tick(buy_price + TP1_MULT * atr)
    tp2 = _ceil_tick(buy_price + atr_tp_mult * atr)
    qty_t1 = max(1, qty // 2)
    if qty < 2:
        log.warning(f"  {c['ticker']} 수량={qty} → 1차 익절 시 전량 청산, 2차 익절 없음")
    return Position(
        ticker=c["ticker"],
        name=c.get("name", c["ticker"]),
        qty=qty,
        buy_price=buy_price,
        atr=atr,
        atr_sl_mult=atr_sl_mult,
        atr_tp_mult=atr_tp_mult,
        sl=sl,
        tp1=tp1,
        tp2=tp2,
        mode=c.get("mode", "REV"),
        max_hold_days=max_hold,
        entry_date=entry_date,
        expiry_date=expiry_date,
        t1_done=False,
        qty_t1=qty_t1,
    )


# ─────────────────────────────────────────
# 포지션 영속화
# ─────────────────────────────────────────
def _load_positions() -> dict[str, Position]:
    if not POSITIONS_FILE.exists():
        return {}
    try:
        data = json.loads(POSITIONS_FILE.read_text())
        today = dtutils.today()
        valid: dict[str, Position] = {}
        for t, v in data.items():
            v.setdefault("realized_pnl", 0.0)
            v.setdefault("atr_sl_mult", fac.ATR_SL_MULT)
            v.setdefault("atr_tp_mult", fac.ATR_TP_MULT)
            v.setdefault("expiry_date", "")
            p = Position(**v)
            if p.max_hold_days == 0 and p.entry_date != today:
                log.warning(f"  이전 데이 포지션 무시: {t} (entry={p.entry_date})")
                continue
            valid[t] = p
        return valid
    except Exception as e:
        log.warning(f"포지션 파일 로드 실패: {e}")
        return {}


def _save_positions(positions: dict[str, Position], swing_only: bool = False):
    """포지션 직렬화 후 원자적 파일 저장. lock 외부에서 호출해야 함."""
    to_save = (
        {t: p for t, p in positions.items() if p.max_hold_days > 0}
        if swing_only
        else positions
    )
    POSITIONS_FILE.parent.mkdir(parents=True, exist_ok=True)
    if to_save:
        tmp = POSITIONS_FILE.with_suffix(".tmp")
        try:
            tmp.write_text(
                json.dumps(
                    {t: asdict(p) for t, p in to_save.items()},
                    ensure_ascii=False,
                    indent=2,
                )
            )
            tmp.replace(POSITIONS_FILE)
        except Exception as e:
            log.error(f"포지션 저장 실패: {e}")
            tmp.unlink(missing_ok=True)
    elif POSITIONS_FILE.exists():
        POSITIONS_FILE.unlink()


def _snapshot_and_save(
    positions: dict[str, Position],
    lock: threading.Lock,
    swing_only: bool = False,
):
    """lock으로 스냅샷만 취득 후 파일 I/O는 lock 밖에서 실행."""
    with lock:
        snapshot = {t: Position(**asdict(p)) for t, p in positions.items()}
    _save_positions(snapshot, swing_only=swing_only)

# ─────────────────────────────────────────
# SL/TP 처리 (lock 외부에서 호출)
# ─────────────────────────────────────────
def _sell_or_log(_api: _Api, pos: Position, qty: int, reason: str) -> bool:
    if _api._sell(pos.ticker, qty, reason):
        _save_trade("sell", pos.ticker, pos.name, qty, 0, reason)
        return True
    log.critical(f"  🚨 매도 실패: {pos.ticker} [{reason}] → 다음 틱 재시도")
    return False


def _process_position(_api: _Api, pos: Position, current: float) -> bool:
    """현재가 기준 SL/TP 처리. lock 외부에서 호출. 포지션 완전 청산 시 True."""
    ret_pct = (current - pos.buy_price) / pos.buy_price * 100

    # 트레일링 SL (주가 상승 시에만 상향)
    trail_sl = _floor_tick(current - pos.atr_sl_mult * pos.atr)
    if trail_sl > pos.sl and trail_sl < current:
        log.info(
            f"  🔺 트레일링 SL: {pos.ticker}  {pos.sl:,.0f} → {trail_sl:,.0f}"
            f"  (현재={current:,.0f})"
        )
        pos.sl = trail_sl

    # 손절
    if current <= pos.sl:
        reason = f"손절 {ret_pct:+.2f}% (SL={pos.sl:,.0f})"
        if _sell_or_log(_api, pos, pos.qty, reason):
            pos.realized_pnl += (
                current - pos.buy_price
            ) * pos.qty - current * pos.qty * SELL_COST_RATE
            pos.qty = 0
        return pos.qty == 0

    # 1차 익절
    if not pos.t1_done and current >= pos.tp1:
        qty_sell = pos.qty_t1
        reason = f"1차익절 {ret_pct:+.2f}% (TP1={pos.tp1:,.0f})"
        if _sell_or_log(_api, pos, qty_sell, reason):
            pos.realized_pnl += (
                current - pos.buy_price
            ) * qty_sell - current * qty_sell * SELL_COST_RATE
            pos.qty -= qty_sell
            pos.t1_done = True
            if pos.buy_price > pos.sl:
                log.info(
                    f"  🔒 SL 본전 이동: {pos.ticker}  {pos.sl:,.0f} → {pos.buy_price:,.0f}"
                )
                pos.sl = pos.buy_price
        return pos.qty == 0

    # 2차 익절
    if current >= pos.tp2:
        reason = f"2차익절 {ret_pct:+.2f}% (TP2={pos.tp2:,.0f})"
        if _sell_or_log(_api, pos, pos.qty, reason):
            pos.realized_pnl += (
                current - pos.buy_price
            ) * pos.qty - current * pos.qty * SELL_COST_RATE
            pos.qty = 0
        return pos.qty == 0

    return False


# ─────────────────────────────────────────
# 매수 (백그라운드 스레드)
# ─────────────────────────────────────────
def _po_task(
    positions: dict[str, Position],
    lock: threading.Lock,
    _api: _Api,
    po
):
    candidates = po.candidates

    with lock:
        held = set(positions.keys())
    holdings, cash = _api._get_balance()
    held |= set(holdings.keys())

    new_cands = [c for c in candidates if c["ticker"] not in held]
    if not new_cands:
        log.info(f"{po.no} 신규 매수 대상 없음")
        return

    prices = _api._fetch_prices([c["ticker"] for c in new_cands])
    valid_cands: list[tuple[dict, float]] = []
    for c in new_cands:
        t = c["ticker"]
        cur = prices.get(t)
        if cur is None:
            log.warning(f"  현재가 없음 ({t}) → 스킵")
            continue
        entry = float(c["entry_price"])
        if cur > entry:
            log.info(f"  ⚠️  갭상승 스킵: {t}  현재={cur:,.0f} > entry={entry:,.0f}")
            continue
        if cur < entry * (1 - GAP_DOWN_LIMIT):
            log.info(f"  ⚠️  갭하락 스킵: {t}  현재={cur:,.0f}")
            continue
        valid_cands.append((c, entry))

    if not valid_cands:
        return

    avail = cash * CASH_USAGE
    alloc = avail / len(valid_cands)
    if alloc < MIN_ALLOC:
        log.warning(f"[10:00] 배분액 {alloc:,.0f}원 < 최소 {MIN_ALLOC:,}원 → 스킵")
        return

    pending: dict[str, dict] = {}
    for c, entry in valid_cands:
        t = c["ticker"]
        qty = max(1, int(alloc // entry))
        odno = _api._buy(t, qty, entry)
        if odno:
            pending[t] = {"cand": c, "odno": odno, "entry_price": entry, "qty": qty}

    # 체결 대기 (CANCEL_MORNING까지)
    while dtutils.ctime() < CANCEL_MORNING and pending:
        holdings, _ = _api._get_balance()
        with lock:
            held_now = set(positions.keys())
        filled = []
        for t, info in pending.items():
            if t in held_now:
                filled.append(t)
                continue
            actual_qty = holdings.get(t, 0)
            if actual_qty > 0:
                if actual_qty < info["qty"]:
                    log.warning(
                        f"  부분 체결: {t}  주문={info['qty']}  체결={actual_qty} → 잔량 취소"
                    )
                    _api._cancel_order(t, info["odno"], info["qty"] - actual_qty)
                pos = _make_position(
                    info["cand"],
                    info["entry_price"],
                    actual_qty,
                    today,
                    info["cand"].get("expiry_date") or "",
                )
                // expiry_date 가 사전에 계산되지 않았으면 당일 청산
                if not pos.expiry_date:
                    pos.expiry_date = dtutils.today()
                with lock:
                    positions[t] = pos
                _save_trade("buy", t, pos.name, actual_qty, info["entry_price"], "지정가체결")
                log.info(
                    f"  ✅ 체결: {t} {pos.name}  매수가={info['entry_price']:,.0f}"
                    f"  SL={pos.sl:,.0f}  TP1={pos.tp1:,.0f}  TP2={pos.tp2:,.0f}"
                    f"  보유={pos.max_hold_days}일"
                )
                filled.append(t)
        for t in filled:
            del pending[t]
        if pending:
            time.sleep(10)

    # 미체결 취소
    for t, info in pending.items():
        log.info(f"  [10:10] 미체결 취소: {t}")
        _api._cancel_order(t, info["odno"], info["qty"])


# ─────────────────────────────────────────
# 오후 스캔+청산+매수 (백그라운드 스레드, 15:20)
# ─────────────────────────────────────────
def _afternoon_task(
    positions: dict[str, Position],
    lock: threading.Lock,
    _api: _Api,
    today: str,
    session_closed: dict[str, Position],
    session_closed_lock: threading.Lock,
):
    log.info("[15:20] 오후 작업 시작 (스캔 + 청산 병렬)")

    scan_result: list[list[dict]] = [[]]

    def do_scan():
        try:
            scan_result[0] = _do_scan(today)
            log.info(f"  [15:20] 스캔 완료: {len(scan_result[0])}종목")
        except Exception as e:
            log.error(f"  [15:20] 스캔 실패: {e}")

    def do_liquidate():
        # lock 범위를 최소화: 대상 목록만 빠르게 복사
        with lock:
            to_liq = [
                (t, Position(**asdict(p)))
                for t, p in positions.items()
                if p.expiry_date == today
            ]
        log.info(f"  [15:20] 만기 청산 대상: {len(to_liq)}종목")
        for ticker, p_snap in to_liq:
            # API 호출은 lock 밖에서 실행
            reason = f"만기청산 (expiry={p_snap.expiry_date})"
            if _api._sell(ticker, p_snap.qty, reason):
                _save_trade("sell", ticker, p_snap.name, p_snap.qty, 0, reason)
                # positions에서 제거: lock만 사용
                with lock:
                    closed_pos = positions.pop(ticker, None)
                # session_closed 등록: lock 없이 session_closed_lock만 사용
                if closed_pos is not None:
                    with session_closed_lock:
                        session_closed[ticker] = closed_pos
                log.info(f"  [15:20] 청산: {ticker} {p_snap.name}  qty={p_snap.qty}")
            else:
                log.warning(f"  [15:20] 청산 실패: {ticker} → 다음 영업일 재시도")

    t1 = threading.Thread(target=do_scan, daemon=True)
    t2 = threading.Thread(target=do_liquidate, daemon=True)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    # 스캔+청산 완료 후 잔고의 1/2로 선별 종목 시장가 매수
    candidates = scan_result[0]
    if not candidates:
        log.info("[15:20] 오후 매수 대상 없음")
        return

    with lock:
        held = set(positions.keys())
    holdings, cash = _api._get_balance()
    held |= set(holdings.keys())

    new_cands = [c for c in candidates if c["ticker"] not in held]
    if not new_cands:
        log.info("[15:20] 신규 매수 대상 없음 (전부 기보유)")
        return

    alloc = (cash * AFTERNOON_CASH_RATIO) / len(new_cands)
    if alloc < MIN_ALLOC:
        log.warning(f"[15:20] 배분액 {alloc:,.0f}원 < 최소 {MIN_ALLOC:,}원 → 스킵")
        return

    prices = _api._fetch_prices([c["ticker"] for c in new_cands])
    for c in new_cands:
        ticker = c["ticker"]
        cur = prices.get(ticker)
        if cur is None:
            log.warning(f"  현재가 없음 ({ticker}) → 스킵")
            continue
        qty = max(1, int(alloc // cur))
        odno = _api._buy_market(ticker, qty)
        if odno:
            pos = _make_position(c, cur, qty, today, c.get("expiry_date") or "")
            if pos.max_hold_days > 0 and not pos.expiry_date:
                pos.expiry_date = _calc_expiry(today, pos.max_hold_days)
            with lock:
                positions[ticker] = pos
            _save_trade("buy", ticker, pos.name, qty, cur, "시장가매수")
            log.info(
                f"  📥 시장가 매수: {ticker} {pos.name}  현재가≈{cur:,.0f}  qty={qty}"
                f"  SL={pos.sl:,.0f}  TP1={pos.tp1:,.0f}  TP2={pos.tp2:,.0f}"
            )


# ─────────────────────────────────────────
# 메인
# ─────────────────────────────────────────
def run():
    print()
    log.info(">>>>> START TRADER2 <<<<<<")
    today = dtutils.today()
    kh = query.get_krx_holiday(today)
    if kh is None:
        log.warning(
            f"krx_holiday에 {today} 데이터 없음. 영업일로 간주하고 계속 진행."
        )
    elif kh["opnd_yn"] != "Y":
        log.info("영업일이 아닙니다.")
        return

    kis_env = os.environ.get("KIS_ENV", "demo").lower()
    try:
        _api = _Api(kis_env, POLL_SEC)
    except RuntimeError as e:
        log.error(str(e))
        return

    _init_trade_history()

    # 포지션 로드 + 구버전 만기일 보정
    positions: dict[str, Position] = _load_positions()
    for p in positions.values():
        if not p.expiry_date and p.max_hold_days > 0:
            p.expiry_date = _calc_expiry(p.entry_date, p.max_hold_days)
    if positions:
        log.info(f"[포지션 로드] {len(positions)}종목")

    # 장 시작 대기
    if dtutils.ctime() < "090000":
        log.info("[대기] 09:00 장 시작 대기…")
        while dtutils.ctime() < "090000":
            time.sleep(5)

    # 기간 초과 포지션 즉시 청산 (전일 만기가 지난 스윙 포지션)
    overdue = [
        ticker for ticker, p in list(positions.items())
        if p.expiry_date and dtutils.today() > p.expiry_date
    ]
    if overdue:
        log.info(f"[기간초과 청산] {len(overdue)}종목")
        for ticker in overdue:
            pos = positions[ticker]
            if _api._sell(ticker, pos.qty, f"기간초과 (expiry={pos.expiry_date})"):
                _save_trade("sell", ticker, pos.name, pos.qty, 0, "기간초과청산")
                del positions[ticker]
            else:
                log.warning(f"  기간초과 청산 실패: {ticker} → 다음 틱 재시도")

    _save_positions(positions)
    log.info(f"[모니터링] {len(positions)}종목 감시 시작")

    lock = threading.Lock()
    session_closed: dict[str, Position] = {}
    session_closed_lock = threading.Lock()
    morning_started = False
    afternoon_thread: threading.Thread | None = None
    last_status_min = ""
    cur_prices: dict[str, float] = {}  # NameError 방지: 루프 진입 전 초기화

    while True:
        now = dtutils.ctime()

        if now >= MARKET_CLOSE:
            log.info("[종료] 장 마감")
            break

        # 오후 작업이 완료되어야 포지션 없음 종료 가능
        # (청산 후 시장가 매수 단계가 끝나지 않았으면 대기)
        afternoon_done = (
            afternoon_thread is None or not afternoon_thread.is_alive()
        )
        with lock:
            pos_empty = len(positions) == 0
        if pos_empty and morning_started and afternoon_thread is not None and afternoon_done:
            log.info("[모니터링] 전 포지션 청산")
            break

        # 10:00 오전 스캔+매수 시작
        if not morning_started and now >= SCAN_MORNING:
            morning_started = True
            threading.Thread(
                target=_morning_task,
                args=(positions, lock, _api, today),
                daemon=True,
            ).start()

        # 15:20 오후 스캔+청산+매수 시작
        if afternoon_thread is None and now >= SCAN_CLOSING:
            afternoon_thread = threading.Thread(
                target=_afternoon_task,
                args=(positions, lock, _api, today, session_closed, session_closed_lock),
                daemon=True,
            )
            afternoon_thread.start()

        # ── 현재가 조회 + SL/TP 처리
        # lock은 positions 스냅샷 취득 시에만 보유, API 호출은 lock 밖에서 실행
        with lock:
            snapshot = list(positions.items())

        if snapshot:
            cur_prices = _api._fetch_prices([ticker for ticker, _ in snapshot])
            closed: list[str] = []

            for ticker, pos in snapshot:
                # 오후 청산 작업이 이미 positions에서 제거한 종목은 스킵
                with lock:
                    if ticker not in positions:
                        continue

                cur = cur_prices.get(ticker)
                if cur is None:
                    continue

                ret_pct = (cur - pos.buy_price) / pos.buy_price * 100
                log.debug(
                    f"  {ticker} {pos.name[:12]:12s}  현재={cur:,.0f}"
                    f"  SL={pos.sl:,.0f}  TP1={pos.tp1:,.0f}  TP2={pos.tp2:,.0f}"
                    f"  {ret_pct:+.2f}%  {'[T1완료]' if pos.t1_done else ''}"
                )
                # _process_position은 pos 객체를 직접 수정
                # snapshot의 pos가 positions[ticker]와 동일 객체이므로 즉시 반영됨
                if _process_position(_api, pos, cur):
                    closed.append(ticker)

            if closed:
                # lock과 session_closed_lock을 중첩하지 않기 위해 순차 획득
                newly_closed: dict[str, Position] = {}
                with lock:
                    for ticker in closed:
                        if ticker in positions:
                            newly_closed[ticker] = positions.pop(ticker)
                if newly_closed:
                    with session_closed_lock:
                        session_closed.update(newly_closed)
                    _snapshot_and_save(positions, lock)

        # 포지션 현황 로그 (2분마다)
        cur_min = now[2:4]
        if cur_min != last_status_min and int(cur_min) % 2 == 0:
            last_status_min = cur_min
            with lock:
                items = list(positions.items())
            if items:
                log.info(
                    f"[{now[:2]}:{cur_min}] 보유 {len(items)}종목: "
                    + ", ".join(
                        f"{ticker}({p.qty}주 {((cur_prices.get(ticker, p.buy_price) - p.buy_price) / p.buy_price * 100):+.1f}%)"
                        for ticker, p in items
                    )
                )

        # 30초마다 포지션 저장 (lock 밖에서 파일 I/O)
        _snapshot_and_save(positions, lock)

        deadline = time.monotonic() + POLL_SEC
        while time.monotonic() < deadline:
            if dtutils.ctime() >= MARKET_CLOSE:
                break
            time.sleep(1)

    # 당일 결과 요약 (SL/TP 청산 + 만기 청산 모두 포함)
    with session_closed_lock:
        closed_summary = dict(session_closed)
    if closed_summary:
        total_pnl = sum(p.realized_pnl for p in closed_summary.values())
        winners = sum(1 for p in closed_summary.values() if p.realized_pnl > 0)
        log.info(
            f"[당일 결과] 청산 {len(closed_summary)}종목"
            f"  추정손익 {total_pnl:+,.0f}원"
            f"  수익 {winners}/손실 {len(closed_summary) - winners}"
        )
        for ticker, p in closed_summary.items():
            log.info(f"  {ticker} {p.name}  {p.realized_pnl:+,.0f}원")

    # 종료: 스윙 포지션만 파일에 저장
    _snapshot_and_save(positions, lock, swing_only=True)
    with lock:
        swing_remaining = {ticker: p for ticker, p in positions.items() if p.max_hold_days > 0}
    log.info(
        f"[세션 종료] 스윙 잔여={len(swing_remaining)}종목"
        + (f"  → {POSITIONS_FILE}" if swing_remaining else "")
    )
    for ticker, p in swing_remaining.items():
        log.info(
            f"  {ticker} {p.name}  qty={p.qty}  SL={p.sl:,.0f}  TP2={p.tp2:,.0f}"
            f"  만기={p.expiry_date}"
        )


# ─────────────────────────────────────────
# 진입점
# ─────────────────────────────────────────
if __name__ == "__main__":
    kis_env = os.environ.get("KIS_ENV", "demo")
    if kis_env == "real":
        confirm = input(
            "🚨 실전투자(KIS_ENV=real) 모드입니다. 계속하시겠습니까? (yes): "
        )
        if confirm.strip().lower() != "yes":
            raise SystemExit(0)
    run()
