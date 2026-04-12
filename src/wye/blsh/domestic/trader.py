"""
자동 매매 트레이더
─────────────────────────────────────────────────────
실행:
    uv run python -m wye.blsh

환경변수:
    KIS_ENV=demo   모의투자 (기본)
    KIS_ENV=real   실전투자 🚨
    USE_WEBSOCKET=1  웹소켓 실시간 체결가 / 0 끄끔 (기본: 비활성)

투자 전략:
    1. 08:00 NXT 프리마켓 → PO① NXT 지정가 매수 (30%). 실패 종목 → KRX 개장 후 재시도.
    2. 09:00 KRX 개장 → 유령/추적불가 체크 → 기간초과 청산 → SL/TP 모니터링 시작.
       - 손절: 현재가 ≤ SL → KRX 시장가 매도 / NXT 하한가 지정가 매도
       - 1차 익절: TP1 도달 → TP1_RATIO 매도, SL → 매수가(본전)
       - 2차 익절: TP2 도달 → 잔여 전량 매도
       - 트레일링 SL: 주가 상승 시 SL 상향 (현재가 - ATR × ATR_SL_MULT)
    3. ~11:35 PO② 감지 → 잔고 15% 지정가 매수. 10분 후 미체결 취소.
    4. 15:15 만기 청산 → PO③ KRX 지정가 매수 (55% × 90%).
    5. 15:30 KRX 마감 → FIN PO 미체결분 NXT 재발주 → NXT SL/TP + pending 체결 확인.
    6. 20:00 NXT 마감 → 종료. 세션 종료 시 실제 체결가로 PnL 보정 + DB update.
    7. 매수/매도 시 trade_history DB + 텔레그램 알림.

구조:
    단일 스레드 — 차등 주기
    ┌────────────────────────────────────────────────────────┐
    │ 08:00~09:00 (NXT 프리마켓):                            │
    │   PO① 매수 + pending 체결 확인 (30초 간격)            │
    ├────────────────────────────────────────────────────────┤
    │ 09:00~15:30 (KRX 정규장):                              │
    │   매 틱 (10초): 현재가 조회 + SL/TP (KRX 시장가 매도)  │
    │   매 SLOW 틱 (30초): PO 감시 + pending 체결 확인       │
    │   09:00 1회: 유령/추적불가 체크 → 기간초과 청산 (재시도)│
    │   15:15: 만기 청산 + PO③ KRX 매수                     │
    ├────────────────────────────────────────────────────────┤
    │ 15:30~20:00 (NXT 에프터마켓):                          │
    │   FIN PO 미체결 → NXT 재발주                           │
    │   매 틱 (10초): 현재가 조회 + SL/TP (NXT 지정가 매도)  │
    │   매 SLOW 틱 (30초): NXT pending 체결 확인             │
    └────────────────────────────────────────────────────────┘

PO 파일: ~/.blsh/{KIS_ENV}/data/po-{entry_date}-{po_type}.json
─────────────────────────────────────────────────────
"""

import json
import time
from dataclasses import dataclass, asdict
import numpy as np
from wye.blsh.domestic import (
    PO_TYPE_PRE,
    PO_TYPE_INI,
    PO_TYPE_FIN,
    PO,
    Tick,
    Milestone,
)
from wye.blsh.domestic.config import (
    ATR_SL_MULT,
    ATR_TP_MULT,
    ATR_CAP,
    TP1_MULT,
    TP1_RATIO,
    MAX_HOLD_DAYS,
    CASH_USAGE,
    MIN_ALLOC,
    MAX_ALLOC_TIERS,
    MAX_ALLOC_RATIO_DEFAULT,
    SELL_COST_RATE,
    PRE_CASH_RATIO,
    INI_CASH_RATIO,
    FIN_CASH_RATIO,
)
from wye.blsh.domestic.kis_client import KISClient
from wye.blsh.domestic.ws_monitor import PriceMonitor
from wye.blsh.common import dtutils, fileutils, messageutils
from wye.blsh.common.env import DATA_DIR, BACKUP_DIR, KIS_ENV, USE_WEBSOCKET
from wye.blsh.database import query
from wye.blsh import new_logger

log = new_logger(__file__, True)

# ─────────────────────────────────────────
# 설정
# ─────────────────────────────────────────

TICK_SEC = 10  # 메인 루프 주기 (현재가 조회 간격)
FETCH_TIMEOUT = 30  # _fetch_prices as_completed 타임아웃 (종목 많아도 안전)
SLOW_EVERY = 3  # po 감시·체결 확인 = TICK_SEC × SLOW_EVERY (30초)
PO_CANCEL_MIN = 10

POSITIONS_FILE = DATA_DIR / "positions.json"
POSITIONS_BAK = BACKUP_DIR / "positions.json.bak"


# ─────────────────────────────────────────
# 데이터 클래스
# ─────────────────────────────────────────
@dataclass
class Position:
    ticker: str
    name: str
    qty: int
    buy_price: float
    atr: float
    atr_sl_mult: float
    atr_tp_mult: float
    sl: float
    tp1: float
    tp2: float
    mode: str
    max_hold_days: int
    entry_date: str
    expiry_date: str = ""
    t1_done: bool = False
    qty_t1: int = 0
    realized_pnl: float = 0.0
    po_type: str = ""
    excg_cd: str = "KRX"  # 매수 시 거래소 (KRX/NXT)
    sell_fail_count: int = 0  # 연속 매도 실패 횟수 (잔고 재확인 트리거)
    overdue_fail_count: int = 0  # 기간초과 청산 누적 실패 횟수 (5회 초과 시 수동 개입 대기)
    high_since_entry: float = 0.0  # 진입 이후 최고가 (트레일링 SL 기준)


@dataclass
class PendingOrder:
    """체결 대기 중인 지정가 매수 주문."""

    cand: dict
    odno: str
    entry_price: float
    qty: int
    deadline: float
    po_type: str = ""
    excg_cd: str = "KRX"  # 발주 거래소 (KRX/NXT) — 취소 시 일치 필요


# ─────────────────────────────────────────
# 수동 개입 필요 알림 (텔레그램 + log.critical + 일일 리포트 마커)
# ─────────────────────────────────────────
def _alert_manual(category: str, detail: str):
    """수동 개입이 필요한 상황을 통일된 방식으로 알림.

    - log.critical로 기록 ([수동개입] 마커 포함 → log_analyzer가 파싱)
    - 텔레그램 메시지 전송

    Args:
        category: 카테고리 (예: "매도실패", "기간초과", "fallback실패")
        detail: 상세 설명
    """
    msg = f"🚨 [수동개입] {category}: {detail}"
    log.critical(msg)
    try:
        messageutils.send_message(msg)
    except Exception as e:
        log.warning(f"수동 개입 알림 텔레그램 전송 실패: {e}")


# ─────────────────────────────────────────
# 이력 저장 (DB INSERT ~1-5ms, 동기, 스레드 불필요)
# ─────────────────────────────────────────
def _save_history(
    side: str,
    ticker: str,
    name: str,
    qty: int,
    price: float | None,
    reason: str = "",
    po_type: str = "",
):
    try:
        query.save_trade_history(side, ticker, name, qty, price, reason, po_type)
    except Exception as e:
        if KIS_ENV == "real":
            _alert_manual("이력저장실패", f"{name}({ticker}) {side}: {e}")
        else:
            log.warning(f"이력 저장 실패 ({ticker}): {e}")

    price_str = f"{price:,.0f}" if price is not None else "미정"
    messageutils.send_message(f"{name}({ticker}) {qty}주를 {price_str}원에 {side}")


# ─────────────────────────────────────────
# 매도 + SL/TP
# ─────────────────────────────────────────
def _sell_market(
    kis: KISClient,
    ticker: str,
    name: str,
    qty: int,
    reason: str,
    today: str,
    po_type: str = "",
) -> tuple[bool, int]:
    """KRX 시장가 매도 + 체결가/수량 조회 후 이력 저장.

    Returns:
        (success, sold_qty): 주문 성공 여부와 실제 체결된 수량.
        - (True, qty): 전량 체결
        - (True, n) where n < qty: 부분 체결 (호출부는 pos.qty -= n 처리)
        - (True, 0): 주문은 접수됐으나 체결 조회 실패 → 잔고 재확인으로 추정 실패.
          세션 종료 시 update_sell_prices 정정에 의존. 호출부는 실패처럼 취급 권장.
        - (False, 0): 주문 자체 실패 (3회 재시도 모두 실패)
    """
    odno = None
    for attempt in range(1, 4):  # 최대 3회 시도
        odno = kis.sell(ticker, qty, reason)
        if odno:
            break
        if attempt < 3:
            log.warning(
                f"  매도 실패 ({attempt}/3): {name}({ticker}) → {attempt}초 후 재시도"
            )
            time.sleep(attempt)
    if not odno:
        _alert_manual("매도3회실패", f"{name}({ticker}) [{reason}]")
        return False, 0

    fill_price: float | None = None
    fill_qty: int = 0
    for wait in (2, 3, 5):  # 최대 3회: 2초 → 3초 → 5초 대기 후 조회
        time.sleep(wait)
        fill_price, fill_qty = kis.get_fill_summary(ticker, odno, today)
        if fill_qty > 0:
            break
        log.debug(f"  체결 조회 대기 중: {ticker} odno={odno} (대기={wait}s)")

    if fill_qty == 0:
        # 체결 조회 실패 — 세션 종료 시 get_sell_fills + update_sell_prices로 보정됨.
        # 현재가 fallback은 제거: 잘못된 PnL이 일중 리포트에 노출되는 것을 방지.
        log.warning(
            f"  ⚠️ 체결 조회 실패: {name}({ticker}) odno={odno} "
            f"→ 이력 NULL 저장 (세션 종료 시 정정)"
        )
        _alert_manual(
            "체결조회실패",
            f"{name}({ticker}) odno={odno} → DB NULL 저장, 세션 종료 시 보정",
        )
        _save_history("sell", ticker, name, qty, None, reason, po_type)
        return True, 0

    if fill_qty < qty:
        log.warning(
            f"  ⚠️ 부분 체결: {name}({ticker}) {fill_qty}/{qty}주 @ {fill_price:,.0f}"
        )
        _save_history(
            "sell", ticker, name, fill_qty, fill_price,
            f"{reason} (부분체결 {fill_qty}/{qty})", po_type,
        )
        return True, fill_qty

    _save_history("sell", ticker, name, fill_qty, fill_price, reason, po_type)
    return True, fill_qty


_SELL_FAIL_BALANCE_CHECK = 3  # 연속 N회 매도 실패 시 KIS 잔고 재확인
OVERDUE_MAX_RETRIES = 5  # 기간초과 청산 최대 재시도 횟수 (초과 시 수동 개입 대기)


def _recover_odno(kis: KISClient, ticker: str, qty: int, today: str) -> str | None:
    """매수 직후 odno가 빈값일 때 미체결 조회로 odno 복구 시도.

    KIS API가 SOR 분할 등으로 빈 odno를 반환하는 경우 대비.
    동일 조건(ticker/side/qty) 매칭이 2건 이상이면 모호하므로 None 반환
    (잘못된 odno 복구로 엉뚱한 주문을 취소하는 것보다 안전).

    Returns: 복구된 odno 또는 None (실패/모호).
    """
    try:
        time.sleep(0.5)  # 미체결 조회 가능 시점까지 짧은 대기
        matches = []
        for o in kis.get_pending_orders(today):
            if (
                o.get("ticker") == ticker
                and o.get("side") == "매수"
                and int(o.get("qty", 0)) == qty
            ):
                odno = str(o.get("odno", ""))
                if odno:
                    matches.append(odno)
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            log.warning(
                f"[odno 복구] {ticker} 동일 조건 미체결 {len(matches)}건 → 모호로 복구 포기"
            )
    except Exception as e:
        log.debug(f"[odno 복구] {ticker}: {e}")
    return None


def _sell_or_log(
    kis: KISClient,
    pos: Position,
    qty: int,
    reason: str,
    nxt_price: int = 0,
    today: str = "",
) -> tuple[bool, int]:
    """nxt_price > 0 이면 NXT 지정가 매도, 아니면 KRX 시장가 매도.
    연속 실패 시 KIS 잔고 확인 → 유령 포지션이면 True 반환하여 포지션 제거.

    Returns:
        (success, sold_qty).
        NXT 지정가 성공 경로는 (True, qty)로 가정 — 실제 체결은 NXT 세션에서 발생하며,
        부분 체결/미체결은 ghost detection(_restore_positions_from_db) + 세션 종료
        update_sell_prices로 보정됨. 호출부는 sold_qty를 기준으로 pos.qty를 차감한다.
    """
    if nxt_price > 0:
        odno = kis.sell_nxt(pos.ticker, qty, nxt_price, reason)
        if odno is not None:
            pos.sell_fail_count = 0
            note = f"{reason} (지정가. 실제 체결가 미정)"
            if not odno:
                note += " ⚠️odno빈값"
                log.warning(
                    f"  ⚠️ NXT 매도 odno 빈값: {pos.ticker} → 잔고 확인으로 추적"
                )
            # qty 미검증 — NXT 지정가 부분체결은 ghost detection + 세션종료
            # update_sell_prices 정정으로 보정. H4 NXT 경로는 의도적 미검증.
            _save_history(
                "sell",
                pos.ticker,
                pos.name,
                qty,
                nxt_price,
                note,
                pos.po_type,
            )
            return True, qty
        # 매도 API 실패 → 잔고 확인하여 이미 체결 여부 검증
        holdings, _, _ = kis.get_balance()
        remaining = holdings.get(pos.ticker, 0)
        if remaining < pos.qty:
            # 잔고 감소 = 매도 체결됨 (API 응답만 실패)
            sold_qty = pos.qty - remaining
            log.warning(
                f"  ⚠️ 매도 API 실패했으나 체결 감지: {pos.ticker}"
                f"  매도수량={sold_qty} (잔고={remaining})"
            )
            _save_history(
                "sell",
                pos.ticker,
                pos.name,
                sold_qty,
                nxt_price,
                f"{reason} (지연감지, 지정가)",
                pos.po_type,
            )
            pos.sell_fail_count = 0
            if remaining <= 0:
                return True, sold_qty
            pos.qty = remaining
            return False, sold_qty
        pos.sell_fail_count += 1
        log.critical(f"  🚨 매도 실패: {pos.ticker} [{reason}] → 다음 틱 재시도")
        return False, 0
    return _sell_market(
        kis, pos.ticker, pos.name, qty, reason, today or dtutils.today(), pos.po_type
    )


# ─────────────────────────────────────────
# 포지션
# ─────────────────────────────────────────
def _load_positions() -> dict[str, Position]:
    source = POSITIONS_FILE
    if not source.exists():
        if POSITIONS_BAK.exists():
            log.warning("positions.json 없음 → .bak에서 복원 시도")
            source = POSITIONS_BAK
        else:
            return {}
    try:
        data = json.loads(source.read_text())
        today = dtutils.today()
        valid: dict[str, Position] = {}
        for t, v in data.items():
            v.setdefault("realized_pnl", 0.0)
            v.setdefault("atr_sl_mult", ATR_SL_MULT)
            v.setdefault("atr_tp_mult", ATR_TP_MULT)
            v.setdefault("expiry_date", "")
            v.setdefault("po_type", "")
            v.setdefault("excg_cd", "KRX")
            v.setdefault("high_since_entry", 0.0)
            v.pop("sell_fail_count", None)  # 세션 내 임시 값 — 이월 방지
            p = Position(**v)
            # [FIX] expiry_date 미설정 보정 (데이: entry_date, 스윙: +N영업일)
            if not p.expiry_date:
                try:
                    p.expiry_date = dtutils.add_biz_days(p.entry_date, p.max_hold_days)
                except Exception as e:
                    log.warning(f"  expiry_date 보정 실패 ({t}): {e}")
                    p.expiry_date = ""
                if p.expiry_date:
                    log.info(
                        f"  expiry_date 보정: {t}  entry={p.entry_date}"
                        f"  +{p.max_hold_days}d → {p.expiry_date}"
                    )
                else:
                    # 휴장일 DB 미보유 등으로 계산 불가 → 빈값 유지.
                    # 자동 만기/기간초과 청산에서 제외 (호출부 1235/1331은
                    # `if p.expiry_date and ...` 가드 보유). 수동 개입 대기.
                    msg = (
                        f"⚠️ expiry_date 계산 실패 ({t}) entry={p.entry_date} "
                        f"+{p.max_hold_days}d → 자동 청산 비활성. "
                        f"휴장일 DB 확인 후 수동 설정 필요"
                    )
                    log.warning(msg)
                    messageutils.send_message(msg)
            valid[t] = p
        return valid
    except Exception as e:
        log.warning(f"포지션 파일 로드 실패: {e}")
        return {}


def _compute_atr(ohlcv_rows: list[dict], period: int = 14) -> float | None:
    """OHLCV 행 목록(오름차순)에서 ATR 계산 (SMA 기반 근사값). 데이터 부족 시 None.

    scanner.py calc_atr은 EWM(span=14)을 사용하나, 긴급 복원 용도이므로
    단순평균(SMA)으로 충분함. 실제값과 소폭 차이 발생 가능.
    """
    if len(ohlcv_rows) < period + 1:
        return None
    highs = np.array([float(r["high"]) for r in ohlcv_rows])
    lows = np.array([float(r["low"]) for r in ohlcv_rows])
    closes = np.array([float(r["close"]) for r in ohlcv_rows])
    prev_c = np.concatenate([[closes[0]], closes[:-1]])
    tr = np.maximum(
        highs - lows, np.maximum(np.abs(highs - prev_c), np.abs(lows - prev_c))
    )
    return float(np.mean(tr[-period:]))


def _restore_positions_from_db(
    orphans: dict[str, int],
    avg_prices: dict[str, float],
    today: str,
) -> dict[str, Position]:
    """KIS 잔고(orphans) + DB 매매이력으로 Position 복원.

    복원 우선순위:
      1. KIS API 평균단가 → 매수가
      2. DB trade_history → entry_date / name / po_type / t1_done
      3. DB OHLCV → ATR (실패 시 매수가 × 2% fallback)
      4. factor 기본값 → atr_sl_mult / mode / max_hold_days

    복원 불가(매수가 불명) 종목은 결과에서 제외 → 호출측에서 청산 처리.
    """
    tickers = list(orphans.keys())
    db_buys = query.get_latest_buy_history(tickers)
    db_sells = query.get_today_sell_history(tickers, today)

    result: dict[str, Position] = {}
    for ticker, qty in orphans.items():
        # ── 매수가: KIS 평균단가 우선, DB 기록 fallback
        buy_price = float(avg_prices.get(ticker) or 0)
        buy_rec = db_buys.get(ticker)
        if not buy_price and buy_rec and buy_rec.get("price"):
            buy_price = float(buy_rec["price"])
        if not buy_price:
            log.warning(f"  [복원] {ticker} 매수가 불명 → 청산 대상")
            continue

        name = (buy_rec or {}).get("name") or ticker
        entry_date = (buy_rec or {}).get("buy_date") or today
        po_type = (buy_rec or {}).get("po_type") or ""

        # ── t1_done: 당일 1차익절 매도 이력 확인
        sell_rec = db_sells.get(ticker)
        t1_done = bool(sell_rec and "1차익절" in (sell_rec.get("reason") or ""))

        # ── ATR: DB OHLCV 기반, 실패 시 매수가 × 2%
        atr_rows = query.get_recent_ohlcv_for_atr(ticker)
        atr = _compute_atr(atr_rows)
        if not atr:
            atr = buy_price * 0.02
            log.warning(f"  [복원] {ticker} ATR 계산 불가 → 매수가×2% = {atr:.0f}")

        atr_sl_mult = ATR_SL_MULT
        atr_tp_mult = ATR_TP_MULT
        tp1_mult = TP1_MULT
        tp1_ratio = TP1_RATIO
        max_hold = MAX_HOLD_DAYS

        effective_atr = min(atr, buy_price * ATR_CAP)
        sl = Tick.floor_tick(buy_price - atr_sl_mult * effective_atr)
        tp1 = Tick.ceil_tick(buy_price + tp1_mult * effective_atr)
        tp2 = Tick.ceil_tick(buy_price + atr_tp_mult * effective_atr)
        qty_t1 = max(1, int(qty * tp1_ratio))

        is_orphan = buy_rec is None
        if is_orphan:
            expiry_date = ""
        else:
            try:
                expiry_date = dtutils.add_biz_days(entry_date, max_hold) or today
            except Exception:
                expiry_date = today

        pos = Position(
            ticker=ticker,
            name=name,
            qty=qty,
            buy_price=buy_price,
            atr=atr,
            atr_sl_mult=atr_sl_mult,
            atr_tp_mult=atr_tp_mult,
            sl=sl,
            tp1=tp1,
            tp2=tp2,
            mode="REV",
            max_hold_days=max_hold,
            entry_date=entry_date,
            expiry_date=expiry_date,
            t1_done=t1_done,
            qty_t1=qty_t1,
            po_type=po_type,
        )
        result[ticker] = pos
        label = "🔸 orphan" if is_orphan else "✅"
        log.info(
            f"  [복원] {label} {ticker} {name}  매수가={buy_price:,.0f}  ATR={atr:.0f}"
            f"  SL={sl:,.0f}  TP1={tp1:,.0f}  TP2={tp2:,.0f}"
            f"  entry={entry_date}  expiry={expiry_date or '(없음)'}"
        )

    return result


def _save_positions(positions: dict[str, Position], swing_only: bool = False):
    to_save = {
        t: asdict(p)
        for t, p in positions.items()
        if not swing_only or p.max_hold_days > 0
    }
    if to_save:
        # 원자적 저장: 임시 파일에 쓴 뒤 rename (크래시 시 기존 파일 보존)
        tmp = POSITIONS_FILE.with_suffix(".tmp")
        try:
            fileutils.create_json(tmp, to_save)
            if POSITIONS_FILE.exists():
                import shutil

                shutil.copy2(POSITIONS_FILE, POSITIONS_BAK)
            tmp.rename(POSITIONS_FILE)
        except Exception as e:
            log.warning(f"positions 저장 실패: {e}")
            tmp.unlink(missing_ok=True)
    elif POSITIONS_FILE.exists():
        POSITIONS_FILE.unlink()


def _make_position(
    c: dict,
    buy_price: float,
    qty: int,
    entry_date: str,
    expiry_date: str = "",
    po_type: str = "",
    excg_cd: str = "KRX",
    ticker: str = "",
) -> Position:
    """po.json dict → Position 생성.

    Args:
        ticker: 종목코드. PO JSON은 {ticker: {atr, ...}} 구조이므로
                value dict에 ticker 키가 없을 수 있어 별도 전달.
    """
    atr = float(c["atr"])
    atr_sl_mult = float(
        c["atr_sl_mult"] if c.get("atr_sl_mult") is not None else ATR_SL_MULT
    )
    atr_tp_mult = float(
        c["atr_tp_mult"] if c.get("atr_tp_mult") is not None else ATR_TP_MULT
    )

    if c.get("max_hold_days") is not None:
        max_hold = int(c["max_hold_days"])
    elif expiry_date and expiry_date > entry_date:
        max_hold = 1
    else:
        max_hold = 0

    if not expiry_date:
        try:
            expiry_date = dtutils.add_biz_days(entry_date, max_hold) or entry_date
        except Exception as e:
            log.warning(f"  expiry_date 계산 실패 ({c.get('ticker')}): {e}")
            expiry_date = entry_date

    tp1_mult = float(c["tp1_mult"] if c.get("tp1_mult") is not None else TP1_MULT)
    tp1_ratio = float(c["tp1_ratio"] if c.get("tp1_ratio") is not None else TP1_RATIO)
    effective_atr = min(atr, buy_price * ATR_CAP)
    sl = Tick.floor_tick(buy_price - atr_sl_mult * effective_atr)
    tp1 = Tick.ceil_tick(buy_price + tp1_mult * effective_atr)
    tp2 = Tick.ceil_tick(buy_price + atr_tp_mult * effective_atr)
    qty_t1 = max(1, int(qty * tp1_ratio))
    if qty_t1 >= qty:
        qty_t1 = qty  # tp1_ratio=1.0 → 전량 청산

    _ticker = ticker or c.get("ticker", "")
    if qty < 2 and tp1_ratio < 1.0:
        log.warning(f"  {_ticker} 수량={qty} → 1차 익절 시 전량 청산, 2차 익절 없음")

    return Position(
        ticker=_ticker,
        name=c.get("name", _ticker),
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
        po_type=po_type,
        excg_cd=excg_cd,
        high_since_entry=buy_price,
    )


def _process_position(
    kis: KISClient, pos: Position, current: float, today: str, nxt_mode: bool = False
) -> tuple[bool, bool]:
    """현재가 기준 SL/TP 처리.

    Args:
        nxt_mode: True이면 NXT 에프터마켓 — 지정가 매도 사용
    Returns:
        (closed, changed) — closed: 포지션 완전 청산, changed: SL 등 상태 변경
    """
    ret_pct = (current - pos.buy_price) / pos.buy_price * 100
    changed = False
    # NXT 모드: 익절은 현재가 지정가, 손절은 하한가 지정가 (0이면 KRX 시장가)
    sell_price = Tick.floor_tick(current) if nxt_mode else 0

    # 트레일링 SL: 직전까지의 최고가 기준 (시뮬레이션 _sim_core.py와 동일 순서)
    # _sim_core.py: SL/TP 체크 → prev_high 갱신 (당일 고가는 다음 봉에서 반영)
    # trader.py:   SL/TP 체크 → high_since_entry 갱신 (현재 틱은 다음 틱에서 반영)
    if pos.high_since_entry > 0:
        # ATR_CAP 적용 (sim_one_nb의 effective_atr와 동일)
        eff_atr = min(pos.atr, pos.buy_price * ATR_CAP)
        trail_sl = Tick.floor_tick(pos.high_since_entry - pos.atr_sl_mult * eff_atr)
        if trail_sl > pos.sl and trail_sl < pos.high_since_entry:
            log.info(
                f"  🔺 트레일링 SL: {pos.ticker}  {pos.sl:,.0f} → {trail_sl:,.0f}"
                f"  (최고={pos.high_since_entry:,.0f}, 현재={current:,.0f})"
            )
            pos.sl = trail_sl
            changed = True

    # [FIX] 매도 성공 시에만 changed=True (실패 시 상태 불변)
    if current <= pos.sl:
        # NXT 손절: 거래소 하한가로 지정가 매도 (사실상 시장가 효과)
        if nxt_mode:
            detail = kis.get_price_detail(pos.ticker)
            sl_sell_price = (
                Tick.floor_tick(detail[1])
                if detail and detail[1]
                else Tick.floor_tick(current)
            )
        else:
            sl_sell_price = 0  # KRX 시장가
        reason = f"손절 {ret_pct:+.2f}% (SL={pos.sl:,.0f})"
        ok, sold = _sell_or_log(
            kis, pos, pos.qty, reason, nxt_price=sl_sell_price, today=today
        )
        if ok:
            pos.realized_pnl += (
                current - pos.buy_price
            ) * sold - current * sold * SELL_COST_RATE
            pos.qty -= sold
            return pos.qty == 0, True
        return False, changed

    if not pos.t1_done and current >= pos.tp1:
        qty_sell = min(pos.qty_t1, pos.qty)  # DB 복원 포지션 qty 불일치 방어
        reason = f"1차익절 {ret_pct:+.2f}% (TP1={pos.tp1:,.0f})"
        ok, sold = _sell_or_log(
            kis, pos, qty_sell, reason, nxt_price=sell_price, today=today
        )
        if ok:
            pos.realized_pnl += (
                current - pos.buy_price
            ) * sold - current * sold * SELL_COST_RATE
            pos.qty -= sold
            # 부분 체결 시 t1_done=True로 마킹해도 qty_t1이 이미 발주됐으므로 안전
            pos.t1_done = True
            if pos.buy_price > pos.sl:
                log.info(
                    f"  🔒 SL 본전 이동: {pos.ticker}  {pos.sl:,.0f} → {pos.buy_price:,.0f}"
                )
                pos.sl = pos.buy_price
            return pos.qty == 0, True
        return False, changed

    if current >= pos.tp2:
        reason = f"2차익절 {ret_pct:+.2f}% (TP2={pos.tp2:,.0f})"
        ok, sold = _sell_or_log(
            kis, pos, pos.qty, reason, nxt_price=sell_price, today=today
        )
        if ok:
            pos.realized_pnl += (
                current - pos.buy_price
            ) * sold - current * sold * SELL_COST_RATE
            pos.qty -= sold
            return pos.qty == 0, True
        return False, changed

    # 최고가 갱신: SL/TP 체크 후 (sim_core.py의 prev_high 갱신 위치와 동일)
    if current > pos.high_since_entry:
        pos.high_since_entry = current
        changed = True

    return False, changed


# ─────────────────────────────────────────
# 주문 관리
# ─────────────────────────────────────────
def _submit_buy_orders(
    orders: dict[str, dict],
    positions: dict[str, Position],
    pending: dict[str, PendingOrder],
    kis: KISClient,
    cash_usage: float = CASH_USAGE,
    cash_limit: float | None = None,
    po_type: str = "",
    excg_id_dvsn_cd: str = "KRX",
) -> dict[str, dict]:
    """기 보유/진행 중 종목 제외 → 배분액 계산 → 지정가 매수 → pending 등록.

    Args:
        cash_usage: 가용 현금 대비 사용 비율 (cash_limit 미지정 시 적용)
        cash_limit: 절대 금액 상한 (지정 시 cash_usage 무시)
        po_type: PO 유형 (pre/morning/final)

    Returns:
        주문 실패 종목 dict {ticker: order_dict} (KRX 개장 후 재시도용)
    """
    failed: dict[str, dict] = {}
    holdings_api, avg_prices, cash = kis.get_balance()
    held = set(positions.keys()) | set(holdings_api.keys()) | set(pending.keys())

    new_orders = {t: o for t, o in orders.items() if t not in held}
    if not new_orders:
        log.info("[po] 신규 매수 대상 없음 (전부 기보유/진행중)")
        return failed

    avail = cash_limit if cash_limit is not None else cash * cash_usage
    stock_value = sum(qty * avg_prices.get(t, 0) for t, qty in holdings_api.items())
    total_asset = cash + stock_value
    ratio = MAX_ALLOC_RATIO_DEFAULT
    for threshold, r in MAX_ALLOC_TIERS:
        if total_asset < threshold:
            ratio = r
            break
    max_alloc = total_asset * ratio
    alloc = min(avail / len(new_orders), max_alloc)
    log.info(
        f"[po] 총자산={total_asset:,.0f} 가용={avail:,.0f} "
        f"종목수={len(new_orders)} 균등={avail / len(new_orders):,.0f} "
        f"상한={max_alloc:,.0f} → 배분={alloc:,.0f}"
    )
    if alloc < MIN_ALLOC:
        msg = f"[po] 배분액 {alloc:,.0f}원 < 최소 {MIN_ALLOC:,}원 → 스킵"
        log.warning(msg)
        messageutils.send_message(msg)
        return failed

    deadline = time.monotonic() + PO_CANCEL_MIN * 60

    for ticker, o in new_orders.items():
        entry_price = float(o.get("entry_price") or o.get("price") or 0)
        if entry_price <= 0:
            log.warning(f"[po] entry_price 없음 ({ticker}) → 스킵")
            continue
        qty = int(alloc // entry_price)
        if qty < 1:
            log.warning(
                f"[po] 배분액 부족: {ticker}  alloc={alloc:,.0f} < price={entry_price:,.0f} → 스킵"
            )
            continue
        odno = kis.buy(ticker, qty, entry_price, excg_id_dvsn_cd)
        if odno is None:
            # API 오류 (exception 또는 빈 응답) → 진짜 실패
            failed[ticker] = o
            log.warning(f"  [po] {excg_id_dvsn_cd} 주문 실패: {ticker}")
            continue
        if not odno:
            # odno="" (API 성공이나 주문번호 빈값) → 미체결 조회로 복구 시도
            log.warning(f"  ⚠️ [po] 주문번호 빈값: {ticker} → 복구 시도")
            recovered = _recover_odno(kis, ticker, qty, dtutils.today())
            if recovered:
                odno = recovered
                log.info(f"  ✅ [po] {ticker} odno 복구 성공: {recovered}")
            else:
                # 즉시 잔고 재확인 — 이미 체결됐으면 Position 직접 생성
                log.warning(
                    f"  ❌ [po] {ticker} odno 복구 실패 → 즉시 잔고 재확인"
                )
                hold_after, avg_after, _ = kis.get_balance()
                filled_qty = hold_after.get(ticker, 0)
                if filled_qty > 0:
                    if filled_qty < qty:
                        log.warning(
                            f"  ⚠️ 부분 체결 감지: {ticker}  주문={qty}"
                            f"  체결={filled_qty}  (odno 미확인 → 잔량 취소 불가)"
                        )
                    buy_price = avg_after.get(ticker) or entry_price
                    try:
                        pos = _make_position(
                            o,
                            buy_price,
                            filled_qty,
                            dtutils.today(),
                            o.get("expiry_date") or "",
                            po_type=po_type,
                            excg_cd=excg_id_dvsn_cd,
                            ticker=ticker,
                        )
                    except Exception as e:
                        log.error(
                            f"  Position 생성 실패 ({ticker}): {e}"
                            f" → UNKNOWN pending으로 fallback"
                        )
                    else:
                        positions[ticker] = pos
                        _save_history(
                            "buy",
                            ticker,
                            pos.name,
                            filled_qty,
                            buy_price,
                            "po즉시체결감지",
                            pos.po_type,
                        )
                        log.info(
                            f"  ✅ 즉시 체결 감지: {ticker} {pos.name}"
                            f"  {filled_qty}주 @ {buy_price:,.0f}"
                            f"  SL={pos.sl:,.0f}  TP1={pos.tp1:,.0f}"
                            f"  TP2={pos.tp2:,.0f}"
                        )
                        continue  # pending 등록 생략
                # 잔고에도 없음 → 실제 미체결 → UNKNOWN 등록 (기존 동작)
                log.warning(
                    f"  ❌ [po] {ticker} 잔고에도 없음 → UNKNOWN 등록"
                    f" (취소 불가, deadline 도달 후 잔고 확인으로 처리)"
                )
        pending[ticker] = PendingOrder(
            cand=o,
            odno=odno or "UNKNOWN",
            entry_price=entry_price,
            qty=qty,
            deadline=deadline,
            po_type=po_type,
            excg_cd=excg_id_dvsn_cd,
        )

    if failed:
        msg = f"⚠️ 매수 주문 실패: {', '.join(failed.keys())} ({excg_id_dvsn_cd})"
        messageutils.send_message(msg)
    return failed


def _check_pending_orders(
    pending: dict[str, PendingOrder],
    positions: dict[str, Position],
    kis: KISClient,
    today: str,
) -> bool:
    """체결 확인 + 시간 초과 취소. 변동 있으면 True."""
    if not pending:
        return False

    holdings_api, avg_prices, _ = kis.get_balance()
    now_mono = time.monotonic()
    done: list[str] = []
    changed = False

    for ticker, po in pending.items():
        # UNKNOWN odno면 매 polling마다 복구 시도 (체결 전이면 미체결 조회로 odno 회수)
        if po.odno == "UNKNOWN":
            recovered = _recover_odno(kis, ticker, po.qty, today)
            if recovered:
                po.odno = recovered
                log.info(f"  ✅ pending {ticker} odno 복구: {recovered}")

        if ticker in positions:
            log.info(f"  [po] {ticker} 이미 보유 중 → 미체결 주문 취소")
            if po.odno != "UNKNOWN" and not kis.cancel_order(
                ticker, po.odno, po.qty, po.excg_cd
            ):
                log.warning(f"  [po] {ticker} 이미보유 취소 실패 (무시)")
            done.append(ticker)
            continue

        actual_qty = holdings_api.get(ticker, 0)
        if actual_qty > 0:
            if actual_qty < po.qty:
                log.warning(
                    f"  부분 체결: {ticker}  주문={po.qty}  체결={actual_qty} → 잔량 취소"
                )
                if po.odno == "UNKNOWN":
                    log.warning(f"  부분체결 잔량 취소 불가 (odno 미확인): {ticker}")
                elif not kis.cancel_order(
                    ticker, po.odno, po.qty - actual_qty, po.excg_cd, all_qty=False
                ):
                    log.warning(
                        f"  부분체결 잔량 취소 실패: {ticker} (체결분 {actual_qty}주로 포지션 생성)"
                    )

            # 실제 매입단가로 SL/TP 보정 (갭 하락 시 entry_price보다 낮을 수 있음)
            buy_price = avg_prices.get(ticker) or po.entry_price
            if buy_price != po.entry_price:
                log.info(
                    f"  📊 매입단가 보정: {ticker}  주문가={po.entry_price:,.0f}"
                    f" → 실제={buy_price:,.0f}"
                )

            try:
                pos = _make_position(
                    po.cand,
                    buy_price,
                    actual_qty,
                    today,
                    po.cand.get("expiry_date") or "",
                    po_type=po.po_type,
                    excg_cd=po.excg_cd,
                    ticker=ticker,
                )
            except Exception as e:
                log.error(f"  Position 생성 실패 ({ticker}): {e}")
                done.append(ticker)
                continue

            positions[ticker] = pos
            _save_history(
                "buy",
                ticker,
                pos.name,
                actual_qty,
                buy_price,
                "po지정가체결",
                pos.po_type,
            )
            log.info(
                f"  ✅ po체결: {ticker} {pos.name}  매수가={buy_price:,.0f}"
                f"  SL={pos.sl:,.0f}  TP1={pos.tp1:,.0f}  TP2={pos.tp2:,.0f}"
                f"  만기={pos.expiry_date or '당일'}"
            )
            done.append(ticker)
            changed = True

        elif now_mono >= po.deadline:
            log.info(f"  [po] {PO_CANCEL_MIN}분 경과 미체결 취소: {ticker}")
            # odno 미확인 주문은 취소 불가 → 잔고 확인으로 직행
            if po.odno == "UNKNOWN":
                log.warning(
                    f"  [po] odno 미확인 → 취소 생략, 잔고 확인: {ticker}"
                )
            elif kis.cancel_order(ticker, po.odno, po.qty, po.excg_cd):
                done.append(ticker)
                continue

            # 취소 실패 또는 odno 미확인 → 이미 체결 가능성, 잔고 재확인
            log.warning(f"  [po] 잔고 재확인: {ticker}")
            holdings_api, avg_prices, _ = kis.get_balance()
            filled_qty = holdings_api.get(ticker, 0)
            if filled_qty > 0:
                buy_price = avg_prices.get(ticker) or po.entry_price
                log.info(
                    f"  [po] 취소실패+잔고확인 → 체결 감지: {ticker}"
                    f"  {filled_qty}주 @ {buy_price:,.0f}"
                )
                try:
                    pos = _make_position(
                        po.cand,
                        buy_price,
                        filled_qty,
                        today,
                        po.cand.get("expiry_date") or "",
                        po_type=po.po_type,
                        excg_cd=po.excg_cd,
                        ticker=ticker,
                    )
                    positions[ticker] = pos
                    _save_history(
                        "buy",
                        ticker,
                        pos.name,
                        filled_qty,
                        buy_price,
                        "po지정가체결(지연감지)",
                        pos.po_type,
                    )
                    log.info(
                        f"  ✅ 지연체결: {ticker} {pos.name}  매수가={buy_price:,.0f}"
                        f"  SL={pos.sl:,.0f}  TP1={pos.tp1:,.0f}  TP2={pos.tp2:,.0f}"
                    )
                    changed = True
                except Exception as e:
                    log.error(f"  지연체결 Position 생성 실패 ({ticker}): {e}")
            else:
                log.warning(f"  [po] 취소 실패 + 잔고 없음: {ticker} (무시)")
            done.append(ticker)

    for t in done:
        del pending[t]

    return changed


def _cancel_all_pending(pending: dict[str, PendingOrder], kis: KISClient):
    for ticker, po in pending.items():
        kis.cancel_order(ticker, po.odno, po.qty, po.excg_cd)
    pending.clear()


# ─────────────────────────────────────────
# 메인
# ─────────────────────────────────────────
def run():
    today = dtutils.today()

    kh = query.get_krx_holiday(today)
    if kh is None:
        log.warning(f"krx_holiday에 {today} 데이터 없음. 개장일로 간주하고 계속 진행.")
    elif kh["opnd_yn"] != "Y":
        log.info(f"[{today}] 개장일이 아닙니다.")
        return

    ctime = dtutils.ctime()
    if ctime >= Milestone.NXT_CLOSE_TIME:
        log.info(f"{ctime} 거래시간이 아닙니다 (NXT 마감 후).")
        return

    try:
        kis = KISClient(KIS_ENV, FETCH_TIMEOUT)
    except RuntimeError as e:
        log.error(str(e))
        return

    mode_label = "🚨 실전투자" if KIS_ENV == "real" else "📋 모의투자"
    log.info(f"[{today}] 트레이더 시작 ({mode_label})")
    holdings, avg_prices, cash = kis.get_balance()
    _stock_value = sum(qty * avg_prices.get(t, 0) for t, qty in holdings.items())
    log.info(
        f"[초기 잔고] 현금={cash:,.0f}  보유={len(holdings)}종목"
        f"  총자산={cash + _stock_value:,.0f}"
    )

    # ── 보유종목 및 잔고 텔레그램 알림
    lines = [f"[{today}] {mode_label}"]
    if holdings:
        lines.append(f"보유 {len(holdings)}종목:")
        for ticker, qty in holdings.items():
            avg = avg_prices.get(ticker, 0)
            lines.append(f"  {ticker} {qty}주 @{avg:,.0f}")
    else:
        lines.append("보유종목 없음")
    lines.append(f"현금: {cash:,.0f}원")
    messageutils.send_message("\n".join(lines))

    # ── 실시간 가격 모니터 (WS + REST 하이브리드)
    monitor = PriceMonitor(kis, use_ws=USE_WEBSOCKET)

    # ── 포지션 로드
    positions: dict[str, Position] = _load_positions()
    if positions:
        log.info(f"[포지션 로드] {len(positions)}종목")

    # ── 08:00 대기 (NXT 프리마켓 개장 — SOR 매수 가능)
    if dtutils.ctime() < Milestone.NXT_OPEN_TIME:
        log.info(f"[대기] {Milestone.NXT_OPEN_TIME[:2]}:00 NXT 프리마켓 대기…")
        while dtutils.ctime() < Milestone.NXT_OPEN_TIME:
            time.sleep(5)

    # ── 웹소켓 모니터 시작 (보유종목 구독)
    monitor.start(list(positions.keys()))

    # ── 상태 변수 (메인 루프 전 초기화 — pre po 처리에서 pending_po 필요)
    pending_po: dict[str, PendingOrder] = {}
    session_closed: dict[str, Position] = {}
    ini_po_bought = False
    liquidated = False
    krx_closed = False
    orphan_done = False
    dirty = False
    tick_count = 0
    last_status_min = ""
    cur_prices: dict[str, float] = {}

    # ── PO① 전일 스캔 (pre po) 매수
    po = PO(PO_TYPE_PRE)
    if po.exists():
        orders = po.loads()
        log.info(f"[매수] {po.path.name} {len(orders)} 종목")
        failed = _submit_buy_orders(
            orders,
            positions,
            pending_po,
            kis,
            cash_usage=PRE_CASH_RATIO,
            po_type=PO_TYPE_PRE,
            excg_id_dvsn_cd="NXT",
        )
        if failed:
            log.info(f"  주문실패 {len(failed)}종목 → KRX 개장 후 재시도")
    else:
        failed = {}

    retry_orders: dict[str, dict] = failed  # KRX 개장 후 재시도 대상
    retry_done = False

    # ── 기간 초과 포지션: 메인 루프에서 유령 체크 후 처리 (유령 매도 시도 방지)
    overdue_done = False

    _save_positions(positions)
    log.info(f"[모니터링] {len(positions)}종목 감시 시작  (틱={TICK_SEC}s)")

    # ── 장 중 메인 루프
    try:
        while True:
            now = dtutils.ctime()
            is_slow_tick = tick_count % SLOW_EVERY == 0
            # [FIX] 포지션/pending 없으면 느린 틱 주기로 전환 (빈 10초 틱 방지)
            has_active = bool(positions) or bool(pending_po)

            # ── NXT 마감 (20:00) → 종료
            if now >= Milestone.NXT_CLOSE_TIME:
                _cancel_all_pending(pending_po, kis)
                log.info("[종료] NXT 마감")
                break

            # ── KRX 마감 (15:30) → KRX 미체결 취소, FIN PO NXT 재발주
            if not krx_closed and now >= Milestone.KRX_CLOSE_TIME:
                # FIN PO 미체결분 NXT 재발주 준비
                fin_retry: dict[str, dict] = {}
                if pending_po:
                    # 체결 확인 (마지막 한 번)
                    _check_pending_orders(pending_po, positions, kis, today)
                    # 남은 FIN PO 미체결 주문 → NXT 재발주 대상 수집
                    for t, po_item in list(pending_po.items()):
                        if po_item.po_type == PO_TYPE_FIN:
                            fin_retry[t] = po_item.cand
                _cancel_all_pending(pending_po, kis)

                # FIN PO NXT 재발주
                if fin_retry:
                    log.info(
                        f"[KRX 마감] FIN PO 미체결 {len(fin_retry)}종목 → NXT 재발주"
                    )
                    _, _, cash = kis.get_balance()
                    cash_limit = cash * FIN_CASH_RATIO * CASH_USAGE
                    _submit_buy_orders(
                        fin_retry,
                        positions,
                        pending_po,
                        kis,
                        cash_limit=cash_limit,
                        po_type=PO_TYPE_FIN,
                        excg_id_dvsn_cd="NXT",
                    )

                krx_closed = True
                if positions or pending_po:
                    log.info(
                        f"[KRX 마감] NXT 에프터마켓 전환"
                        f"  (보유={len(positions)}종목, 체결대기={len(pending_po)}종목,"
                        f" ~{Milestone.NXT_CLOSE_TIME[:2]}:00)"
                    )
                else:
                    log.info("[KRX 마감] 보유/체결대기 종목 없음 → 종료")
                    break

            # ── 0. KRX 개장 시: 프리마켓 재시도 → 유령 체크 → 기간초과 청산
            krx_open = now >= Milestone.KRX_OPEN_TIME and not krx_closed

            # 프리마켓 주문 실패 종목 재시도 (KRX 개장 후 1회)
            if krx_open and not retry_done and retry_orders:
                log.info(f"[KRX 개장] 프리마켓 실패 {len(retry_orders)}종목 재주문")
                still_failed = _submit_buy_orders(
                    retry_orders,
                    positions,
                    pending_po,
                    kis,
                    cash_usage=PRE_CASH_RATIO,
                    po_type=PO_TYPE_PRE,
                )
                if still_failed:
                    _alert_manual(
                        "매수재주문실패",
                        f"프리마켓 실패분 KRX 재시도도 실패: {', '.join(still_failed.keys())}",
                    )
                retry_done = True

            # ── 0-1. 유령/추적불가 체크 (KRX 개장 후 1회, 기간초과 청산보다 먼저 실행)
            if krx_open and not orphan_done:
                orphan_done = True
                holdings_chk, avg_prices_chk, _ = kis.get_balance()
                tracked = (
                    set(positions.keys())
                    | set(pending_po.keys())
                    | set(session_closed.keys())
                )

                # ── 유령 포지션 제거: positions에 있지만 KIS 잔고에 없는 종목
                ghost = {
                    t
                    for t in positions
                    if t not in holdings_chk or holdings_chk[t] <= 0
                }
                if ghost:
                    log.warning(
                        f"[유령 포지션] KIS 잔고에 없는 종목 {len(ghost)}건 → 제거:"
                        f" {list(ghost)}"
                    )

                    for t in ghost:
                        del positions[t]
                    dirty = True
                    monitor.sync_subscriptions(list(positions.keys()))

                orphans = {
                    t: q for t, q in holdings_chk.items() if t not in tracked and q > 0
                }
                if orphans:
                    log.warning(
                        f"[추적불가] positions에 없는 보유종목 {len(orphans)}건 → 복원 시도"
                        f" {list(orphans.keys())}"
                    )

                    restored = _restore_positions_from_db(
                        orphans, avg_prices_chk, today
                    )
                    for ticker, pos in restored.items():
                        positions[ticker] = pos
                        dirty = True
                    if restored:
                        monitor.sync_subscriptions(list(positions.keys()))
                        log.info(
                            f"✅ [추적불가 복원] 복원 성공 {len(restored)}건"
                            f" {list(restored.keys())}"
                        )
                    unrestorable = {
                        t: q for t, q in orphans.items() if t not in restored
                    }
                    if unrestorable:
                        log.warning(
                            f"🚨 [추적불가 청산] 매수가 불명 {len(unrestorable)}건 → 시장가 청산"
                        )
                        for ticker, qty in unrestorable.items():
                            reason = "추적불가(복원실패)"
                            ok, sold = _sell_market(
                                kis, ticker, ticker, qty, reason, today
                            )
                            if ok:
                                if sold == 0:
                                    log.warning(
                                        f"  추적불가 체결조회실패: {ticker}"
                                        f"  {qty}주 (다음 세션에서 재감지)"
                                    )
                                elif sold < qty:
                                    log.warning(
                                        f"  추적불가 청산 부분체결: {ticker}"
                                        f"  {sold}/{qty}주 (잔량은 다음 세션에서 재감지)"
                                    )
                                else:
                                    log.info(f"  추적불가 청산: {ticker}  수량={sold}")
                            else:
                                _alert_manual(
                                    "추적불가청산실패",
                                    f"{ticker} {qty}주 → 시장가 청산 실패",
                                )

            # ── 0-2. 기간초과 청산 (유령 체크 후 실행, 슬로우 틱 간격 재시도)
            if krx_open and not overdue_done and is_slow_tick:
                # 5회 초과 실패한 종목은 자동 재시도 중단 (수동 개입 대기)
                overdue = [
                    t
                    for t, p in list(positions.items())
                    if p.expiry_date and today > p.expiry_date
                    and p.overdue_fail_count < OVERDUE_MAX_RETRIES
                ]
                if overdue:
                    log.info(f"[기간초과 청산] {len(overdue)}종목 시도")
                    for ticker in overdue:
                        pos = positions[ticker]
                        ok, sold = _sell_market(
                            kis,
                            ticker,
                            pos.name,
                            pos.qty,
                            "기간초과청산",
                            today,
                            pos.po_type,
                        )
                        if ok:
                            if sold == 0:
                                # 주문 접수됐으나 체결 조회 실패 → 포지션 유지,
                                # 다음 슬로우 틱에서 overdue 재감지되어 재시도.
                                log.warning(
                                    f"  기간초과 체결조회실패: {ticker}"
                                    f"  → 포지션 유지, 슬로우 틱 재시도"
                                )
                            elif sold < pos.qty:
                                pos.qty -= sold
                                log.warning(
                                    f"  기간초과 부분체결: {ticker} {sold}/{pos.qty + sold}"
                                    f"  → 잔량 {pos.qty}주 다음 슬로우 틱 재시도"
                                )
                            else:
                                session_closed[ticker] = positions.pop(ticker)
                        else:
                            pos.overdue_fail_count += 1
                            if pos.overdue_fail_count >= OVERDUE_MAX_RETRIES:
                                _alert_manual(
                                    "기간초과청산실패",
                                    f"{ticker} {pos.name} {OVERDUE_MAX_RETRIES}회 실패"
                                    f" (만기 {pos.expiry_date}) → 자동 재시도 중단"
                                    f" (거래정지/관리종목 가능성)",
                                )
                            else:
                                log.warning(
                                    f"  기간초과 청산 실패 ({pos.overdue_fail_count}/{OVERDUE_MAX_RETRIES}): "
                                    f"{ticker} → 30초 후 재시도"
                                )
                    dirty = True
                    monitor.sync_subscriptions(list(positions.keys()))
                # 미청산 기간초과 종목이 남아있으면 다음 슬로우 틱에서 재시도
                overdue_done = not overdue

            # ── 1. 현재가 조회 + SL/TP 처리 (KRX 장중 + NXT 에프터마켓)
            if (krx_open or krx_closed) and positions:
                cur_prices = monitor.get_prices(list(positions.keys()))
                closed: list[str] = []

                for ticker, pos in list(positions.items()):
                    cur = cur_prices.get(ticker)
                    if not cur:  # None 또는 0 (API 오류) 모두 스킵
                        continue
                    ret_pct = (cur - pos.buy_price) / pos.buy_price * 100
                    log.debug(
                        f"  {ticker} {pos.name[:12]:12s}  현재={cur:,.0f}"
                        f"  SL={pos.sl:,.0f}  TP1={pos.tp1:,.0f}  TP2={pos.tp2:,.0f}"
                        f"  {ret_pct:+.2f}%  {'[T1완료]' if pos.t1_done else ''}"
                    )
                    is_closed, is_changed = _process_position(
                        kis, pos, cur, today, nxt_mode=krx_closed
                    )
                    if is_closed:
                        closed.append(ticker)
                    if is_changed:
                        dirty = True

                if closed:
                    for ticker in closed:
                        session_closed[ticker] = positions.pop(ticker)
                    monitor.sync_subscriptions(list(positions.keys()))

            # ── 2. po 파일 감시 + pending 체결 확인 (느린 틱, KRX+NXT)
            if is_slow_tick:
                # KRX 장중: INI PO 감시
                if not krx_closed:
                    if not ini_po_bought and now <= Milestone.KRX_EARLY_TIME:
                        po = PO(PO_TYPE_INI)
                        if po.exists():
                            orders = po.loads()
                            log.info(f"[매수] {po.path.name} {len(orders)} 종목")
                            _submit_buy_orders(
                                orders,
                                positions,
                                pending_po,
                                kis,
                                cash_usage=INI_CASH_RATIO,
                                po_type=PO_TYPE_INI,
                            )
                            ini_po_bought = True

                # KRX + NXT: pending 체결 확인
                if pending_po and _check_pending_orders(
                    pending_po, positions, kis, today
                ):
                    dirty = True
                    monitor.sync_subscriptions(list(positions.keys()))

            # ── 3. 만기 청산 (1회)
            if not liquidated and now >= Milestone.LIQUIDATE_TIME:
                log.info("만기 청산 시작")
                to_liq = [
                    (t, p)
                    for t, p in list(positions.items())
                    if p.expiry_date and p.expiry_date <= today
                ]
                log.info(f"  만기 청산 대상: {len(to_liq)}종목")

                for ticker, pos in to_liq:
                    reason = f"만기청산 (expiry={pos.expiry_date})"
                    ok, sold = _sell_market(
                        kis, ticker, pos.name, pos.qty, reason, today, pos.po_type
                    )
                    if ok:
                        if sold == 0:
                            # 주문 접수됐으나 체결 조회 실패 → 포지션 유지,
                            # 다음 영업일 expiry 재감지에서 재시도.
                            log.warning(
                                f"  만기청산 체결조회실패: {ticker}"
                                f"  → 포지션 유지, 다음 영업일 재시도"
                            )
                        elif sold < pos.qty:
                            pos.qty -= sold
                            log.warning(
                                f"  만기청산 부분체결: {ticker} {sold}/{pos.qty + sold}"
                                f"  → 잔량 {pos.qty}주 다음 영업일 재시도"
                            )
                        else:
                            session_closed[ticker] = positions.pop(ticker)
                            log.info(
                                f"  청산: {ticker} {pos.name}  qty={sold}"
                            )
                    else:
                        log.warning(f"  청산 실패: {ticker} → 다음 영업일 재시도")

                # final po 처리
                po = PO(PO_TYPE_FIN)
                if po.exists():
                    orders = po.loads()
                    log.info(f"[매수] {po.path.name} {len(orders)} 종목")
                    time.sleep(2)
                    _, _, cash = kis.get_balance()
                    cash_limit = cash * FIN_CASH_RATIO * CASH_USAGE
                    _submit_buy_orders(
                        orders,
                        positions,
                        pending_po,
                        kis,
                        cash_limit=cash_limit,
                        po_type=PO_TYPE_FIN,
                    )

                liquidated = True
                dirty = True
                monitor.sync_subscriptions(list(positions.keys()))

            # ── 4. 종료 조건 (pending 체결대기도 없어야 종료)
            if not positions and not pending_po and (liquidated or krx_closed):
                log.info("[모니터링] 전 포지션 청산")
                break

            # ── 5. 포지션 저장 (변경 시에만)
            if dirty:
                _save_positions(positions)
                dirty = False

            # ── 6. 2분마다 현황 로그
            cur_min = now[2:4]
            if cur_min != last_status_min and int(cur_min) % 2 == 0:
                last_status_min = cur_min
                items = list(positions.items())
                if items:
                    log.info(
                        f"[{now[:2]}:{cur_min}] 보유 {len(items)}종목: "
                        + ", ".join(
                            f"{t}({p.qty}주 "
                            f"{((cur_prices.get(t, p.buy_price) - p.buy_price) / p.buy_price * 100):+.1f}%)"
                            for t, p in items
                        )
                    )
                if pending_po:
                    log.info(
                        f"  체결대기 {len(pending_po)}종목: "
                        + ", ".join(pending_po.keys())
                    )
                if monitor.ws_active:
                    log.info(f"  WS 모니터: {monitor.ws_ticker_count}종목 구독 중")

            # ── 7. 대기 (포지션 있으면 TICK_SEC, 없으면 SLOW_EVERY×TICK_SEC)
            tick_count += 1
            wait_sec = TICK_SEC if has_active else TICK_SEC * SLOW_EVERY
            deadline = time.monotonic() + wait_sec
            while time.monotonic() < deadline:
                if dtutils.ctime() >= Milestone.NXT_CLOSE_TIME:
                    break
                time.sleep(1)

    finally:
        # ── 포지션 저장 (최우선 — API 호출 없이 즉시 완료)
        _save_positions(positions, swing_only=True)

        # ── 웹소켓 종료
        try:
            monitor.stop()
        except Exception as e:
            log.warning(f"웹소켓 종료 실패: {e}")

        # ── 당일 결과 요약 (실제 체결가로 PnL 보정, 타임아웃 보호)
        try:
            if session_closed:
                # 일시 API 오류 대비 재시도 (2초 간격 3회)
                sell_fills = None
                for attempt in range(1, 4):
                    try:
                        sell_fills = kis.get_sell_fills(today)
                        if sell_fills:
                            break
                    except Exception as e:
                        log.warning(
                            f"  매도체결 조회 실패 ({attempt}/3): {e}"
                        )
                    if attempt < 3:
                        time.sleep(2)
                if sell_fills is None:
                    sell_fills = {}
                if sell_fills:
                    try:
                        query.update_sell_prices(today, sell_fills)
                    except Exception as e:
                        log.warning(f"DB 매도 체결가 보정 실패: {e}")
                for t, p in session_closed.items():
                    if t in sell_fills:
                        avg_price, total_qty = sell_fills[t]
                        old_pnl = p.realized_pnl
                        p.realized_pnl = (
                            avg_price - p.buy_price
                        ) * total_qty - avg_price * total_qty * SELL_COST_RATE
                        if abs(old_pnl - p.realized_pnl) > 1:
                            log.info(
                                f"  PnL 보정: {t}  추정={old_pnl:+,.0f} → 실제={p.realized_pnl:+,.0f}"
                                f"  (평균체결가={avg_price:,.0f}, 수량={total_qty})"
                            )

                total_pnl = sum(p.realized_pnl for p in session_closed.values())
                winners = sum(1 for p in session_closed.values() if p.realized_pnl > 0)
                message = (
                    f"[당일 결과] 청산 {len(session_closed)}종목"
                    f"  손익 {total_pnl:+,.0f}원"
                    f"  수익 {winners}/손실 {len(session_closed) - winners}"
                )
                log.info(message)
                messageutils.send_message(message)

                for t, p in session_closed.items():
                    log.info(f"  {t} {p.name}  {p.realized_pnl:+,.0f}원")
            else:
                message = "[당일 결과] 거래 없음"
                log.info(message)
                messageutils.send_message(message)
        except Exception as e:
            log.warning(f"당일 결과 요약 실패 (position은 이미 저장됨): {e}")
        swing_remaining = {t: p for t, p in positions.items() if p.max_hold_days > 0}
        log.info(
            f"[세션 종료] 스윙 잔여={len(swing_remaining)}종목"
            + (f"  → {POSITIONS_FILE}" if swing_remaining else "")
        )
        for t, p in swing_remaining.items():
            log.info(
                f"  {t} {p.name}  qty={p.qty}  SL={p.sl:,.0f}  TP2={p.tp2:,.0f}"
                f"  만기={p.expiry_date}"
            )
