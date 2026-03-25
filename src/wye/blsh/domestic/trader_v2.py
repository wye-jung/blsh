"""
자동 매매 트레이더 v2
─────────────────────────────────────────────────────
실행:
    uv run python -m blsh.wye.domestic.trader_v2

환경변수:
    KIS_ENV=demo   모의투자 (기본)
    KIS_ENV=real   실전투자 🚨

투자 전략:
    1. 08:00 NXT 프리마켓 개장 → PO①(전일스캔) SOR 매수 (30%).
       09:00 KRX 정규장 개장 → SL/TP 매도 + 기간초과 청산 시작.
       보유종목 모니터링.
        장 중 현재가 조회 → ATR 기반 SL/TP 처리
        - 손절: 현재가 ≤ dynamic_sl → 전량 시장가 매도
        - 1차 익절(TP1 = buy+ATR×1.0): 50% 매도, SL → 매수가(본전 보장)
        - 2차 익절(TP2 = buy+ATR×ATR_TP_MULT): 잔여 전량 매도
        - 트레일링 SL: 주가 상승 시 SL을 (현재가 - ATR×ATR_SL_MULT) 로 상향
    2. 모니터링 중 PO②(오전 스캔) 감지 시 잔고의 15% SOR 매수 (장중 미확정 데이터 → 탐색적)
        json파일은 별도의 프로세스가 생성.
        json파일이 여러개이고 ticker 가 중복되면 나중에 만들어진 json 파일의 ticker 매수 정보로 매수.
        po_오늘_final.json 은 청산 이후 매수
        기 보유종목은 매수 제외.
        10분 후 미체결 주문 취소.
        읽어들여 처리한 json파일은 ~/.blsh/data/po/done 폴더로 이동.
    3. 15:15 청산
        청산 조건
        - 오늘이 청산일인 종목. 청산하지 못하면 다음 영업일 재실행 시 청산.
          포지션은 ~/.blsh/data/positions.json 에 영속 저장
    4. 청산 직후 ~/.blsh/data/po 폴더에 po_오늘_final.json이 존재하면 읽어들여 지정가 매수.
        기 보유종목은 매수 제외.
        PO③(오후 스캔) 청산 후 잔고의 55% 의 90% SOR 매수 (확정에 가까운 데이터 → 주력).
    5. 매수, 매도 성공시 trade_history 테이블에 이력 저장

구조:
    완전 단일 스레드 — 작업별 차등 주기
    ┌──────────────────────────────────────────────────────┐
    │ 매 틱 (TICK_SEC=10초):                               │
    │   → 현재가 조회 + SL/TP 처리 (최우선)               │
    │ 매 SLOW_EVERY틱 (30초):                              │
    │   → po 파일 감시 + 주문 제출                         │
    │   → pending 체결 확인 / 미체결 취소                  │
    │ 이벤트 기반:                                         │
    │   → 포지션 저장 (변경 시에만)                        │
    │   → 15:20 만기 청산 + po_오늘_final 매수 (1회)  │
    └──────────────────────────────────────────────────────┘

po_*.json 포맷 (list 또는 단일 dict):
[
    {
        "ticker":"002360",
        "name":"부산산업",
        "market":"KOSPI",
        "mode":"REV",
        "max_hold_days":3,
        "entry_price":471,
        "stop_loss":339,
        "take_profit":549,
        "atr":52.4954,
        "atr_sl_mult":2.0,
        "atr_tp_mult":2.0,
        "entry_date":"20260320",
        "expiry_date":"20260323"
    }
]
─────────────────────────────────────────────────────
"""

import json
import logging
from logging.handlers import TimedRotatingFileHandler
import os
import time
from dataclasses import dataclass
from wye.blsh.domestic import _tick, _kis, _factor
from wye.blsh.domestic._po import (
    PO_DIR,
    parse_po_file,
    collect_po_orders,
    move_po_file,
    get_pre_po_name,
    get_final_po_name,
)
from wye.blsh.common import dtutils, fileutils, messageutils
from wye.blsh.common.env import DATA_DIR, LOG_DIR
from wye.blsh.database import query

log = logging.getLogger(__name__)
_fh = TimedRotatingFileHandler(
    LOG_DIR / "trader.log", when="midnight", backupCount=30, encoding="utf-8"
)
_fh.suffix = "%Y-%m-%d"
_fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
log.addHandler(_fh)

# ─────────────────────────────────────────
# 설정
# ─────────────────────────────────────────
CASH_USAGE = 0.9  # 가용 현금의 90% 사용
PRE_MARKET_CASH_RATIO = (
    0.30  # PO① 전일 스캔: 가용 현금의 30% (확정 일봉, 갭 리스크 있음)
)
MORNING_CASH_RATIO = (
    0.15  # PO② 오전 스캔: 가용 현금의 15% (장중 미확정 데이터 → 탐색적)
)
AFTERNOON_CASH_RATIO = (
    0.55  # PO③ 오후 스캔: 청산 후 현금의 55% (확정에 가까운 데이터 → 주력)
)
MIN_ALLOC = 10_000  # 종목당 최소 배분액 (1만원)
NXT_PRE_OPEN = "080000"  # NXT 프리마켓 개장 (매수 SOR 가능)
KRX_OPEN = "090000"  # KRX 정규장 개장 (매도 가능)
LIQUIDATE_TIME = "152000"  # 청산시간
MARKET_CLOSE = "153000"  # 장 마감
# TP1_MULT, TP1_RATIO, GAP_DOWN_LIMIT → _factor.py 에서 로드
SELL_COST_RATE = 0.002  # 증권거래세 + 수수료 합산 (약 0.2%)
TICK_SEC = 10  # 메인 루프 주기 (현재가 조회 간격)
FETCH_TIMEOUT = 30  # _fetch_prices as_completed 타임아웃 (종목 많아도 안전)
SLOW_EVERY = 3  # po 감시·체결 확인 = TICK_SEC × SLOW_EVERY (30초)
PO_CANCEL_MIN = 10

POSITIONS_FILE = DATA_DIR / "positions.json"


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


@dataclass
class PendingOrder:
    """체결 대기 중인 지정가 매수 주문."""

    cand: dict
    odno: str
    entry_price: float
    qty: int
    deadline: float
    po_type: str = ""


# ─────────────────────────────────────────
# 이력 저장 (DB INSERT ~1-5ms, 동기, 스레드 불필요)
# ─────────────────────────────────────────
def _save_history(
    side: str,
    ticker: str,
    name: str,
    qty: int,
    price: float,
    reason: str = "",
    po_type: str = "",
):
    try:
        query.save_trade_history(side, ticker, name, qty, price, reason, po_type)
    except Exception as e:
        log.warning(f"이력 저장 실패 ({ticker}): {e}")

    messageutils.send_message(
        " | ".join([side, ticker, name, qty, price, reason, po_type])
    )


# ─────────────────────────────────────────
# 매도 + SL/TP
# ─────────────────────────────────────────
def _sell_or_log(_api: _kis.API, pos: Position, qty: int, reason: str) -> bool:
    if _api.sell(pos.ticker, qty, reason):
        _save_history("sell", pos.ticker, pos.name, qty, 0, reason, pos.po_type)
        return True
    log.critical(f"  🚨 매도 실패: {pos.ticker} [{reason}] → 다음 틱 재시도")
    return False


# ─────────────────────────────────────────
# 포지션
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
            v.setdefault("atr_sl_mult", _factor.ATR_SL_MULT)
            v.setdefault("atr_tp_mult", _factor.ATR_TP_MULT)
            v.setdefault("expiry_date", "")
            v.setdefault("po_type", "")
            p = Position(**v)
            if p.max_hold_days == 0 and p.entry_date != today:
                log.warning(f"  이전 데이 포지션 무시: {t} (entry={p.entry_date})")
                continue
            # [FIX] 구버전 포지션 expiry_date 미설정 보정
            if not p.expiry_date and p.max_hold_days > 0:
                try:
                    p.expiry_date = (
                        dtutils.add_biz_days(p.entry_date, p.max_hold_days)
                        or p.entry_date
                    )
                    log.info(
                        f"  expiry_date 보정: {t}  entry={p.entry_date}"
                        f"  +{p.max_hold_days}d → {p.expiry_date}"
                    )
                except Exception as e:
                    log.warning(f"  expiry_date 보정 실패 ({t}): {e}")
                    p.expiry_date = today  # 안전 fallback: 오늘 청산 대상
            valid[t] = p
        return valid
    except Exception as e:
        log.warning(f"포지션 파일 로드 실패: {e}")
        return {}


def _save_positions(positions: dict[str, Position], swing_only: bool = False):
    to_save = (
        {t: p for t, p in positions.items() if p.max_hold_days > 0}
        if swing_only
        else positions
    )
    if to_save:
        fileutils.create_json(POSITIONS_FILE, to_save)
    elif POSITIONS_FILE.exists():
        POSITIONS_FILE.unlink()


def _make_position(
    c: dict,
    buy_price: float,
    qty: int,
    entry_date: str,
    expiry_date: str = "",
    po_type: str = "",
) -> Position:
    """po.json dict → Position 생성."""
    atr = float(c["atr"])
    atr_sl_mult = float(
        c["atr_sl_mult"] if c.get("atr_sl_mult") is not None else _factor.ATR_SL_MULT
    )
    atr_tp_mult = float(
        c["atr_tp_mult"] if c.get("atr_tp_mult") is not None else _factor.ATR_TP_MULT
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

    tp1_mult = float(
        c["tp1_mult"] if c.get("tp1_mult") is not None else _factor.TP1_MULT
    )
    tp1_ratio = float(
        c["tp1_ratio"] if c.get("tp1_ratio") is not None else _factor.TP1_RATIO
    )
    sl = _tick.floor_tick(buy_price - atr_sl_mult * atr)
    tp1 = _tick.ceil_tick(buy_price + tp1_mult * atr)
    tp2 = _tick.ceil_tick(buy_price + atr_tp_mult * atr)
    qty_t1 = max(1, int(qty * tp1_ratio))
    if qty_t1 >= qty:
        qty_t1 = qty  # tp1_ratio=1.0 → 전량 청산
    if qty < 2 and tp1_ratio < 1.0:
        log.warning(
            f"  {c['ticker']} 수량={qty} → 1차 익절 시 전량 청산, 2차 익절 없음"
        )

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
        po_type=po_type,
    )


def _process_position(
    _api: _kis.API, pos: Position, current: float
) -> tuple[bool, bool]:
    """현재가 기준 SL/TP 처리.

    Returns:
        (closed, changed) — closed: 포지션 완전 청산, changed: SL 등 상태 변경
    """
    ret_pct = (current - pos.buy_price) / pos.buy_price * 100
    changed = False

    trail_sl = _tick.floor_tick(current - pos.atr_sl_mult * pos.atr)
    if trail_sl > pos.sl and trail_sl < current:
        log.info(
            f"  🔺 트레일링 SL: {pos.ticker}  {pos.sl:,.0f} → {trail_sl:,.0f}"
            f"  (현재={current:,.0f})"
        )
        pos.sl = trail_sl
        changed = True

    # [FIX] 매도 성공 시에만 changed=True (실패 시 상태 불변)
    if current <= pos.sl:
        reason = f"손절 {ret_pct:+.2f}% (SL={pos.sl:,.0f})"
        if _sell_or_log(_api, pos, pos.qty, reason):
            pos.realized_pnl += (
                current - pos.buy_price
            ) * pos.qty - current * pos.qty * SELL_COST_RATE
            pos.qty = 0
            return True, True
        return False, changed

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
            return pos.qty == 0, True
        return False, changed

    if current >= pos.tp2:
        reason = f"2차익절 {ret_pct:+.2f}% (TP2={pos.tp2:,.0f})"
        if _sell_or_log(_api, pos, pos.qty, reason):
            pos.realized_pnl += (
                current - pos.buy_price
            ) * pos.qty - current * pos.qty * SELL_COST_RATE
            pos.qty = 0
            return True, True
        return False, changed

    return False, changed


# ─────────────────────────────────────────
# 주문 관리
# ─────────────────────────────────────────
def _submit_buy_orders(
    orders: dict[str, dict],
    positions: dict[str, Position],
    pending: dict[str, PendingOrder],
    _api: _kis.API,
    today: str,
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
    holdings_api, avg_prices, cash = _api.get_balance()
    held = set(positions.keys()) | set(holdings_api.keys()) | set(pending.keys())

    new_orders = {t: o for t, o in orders.items() if t not in held}
    if not new_orders:
        log.info("[po] 신규 매수 대상 없음 (전부 기보유/진행중)")
        return failed

    avail = cash_limit if cash_limit is not None else cash * cash_usage
    alloc = avail / len(new_orders)
    if alloc < MIN_ALLOC:
        log.warning(f"[po] 배분액 {alloc:,.0f}원 < 최소 {MIN_ALLOC:,}원 → 스킵")
        return failed

    deadline = time.monotonic() + PO_CANCEL_MIN * 60

    for ticker, o in new_orders.items():
        entry_price = float(o.get("entry_price") or o.get("price") or 0)
        if entry_price <= 0:
            log.warning(f"[po] entry_price 없음 ({ticker}) → 스킵")
            continue
        qty = max(1, int(alloc // entry_price))
        odno = _api.buy(ticker, qty, entry_price, excg_id_dvsn_cd)
        if odno:
            pending[ticker] = PendingOrder(
                cand=o,
                odno=odno,
                entry_price=entry_price,
                qty=qty,
                deadline=deadline,
                po_type=po_type,
            )
        else:
            failed[ticker] = o
            log.warning(f"  [po] 주문 실패: {ticker} → KRX 개장 후 재시도 대상")

    return failed


def _check_pending_orders(
    pending: dict[str, PendingOrder],
    positions: dict[str, Position],
    _api: _kis.API,
    today: str,
) -> bool:
    """체결 확인 + 시간 초과 취소. 변동 있으면 True."""
    if not pending:
        return False

    holdings_api, avg_prices, _ = _api.get_balance()
    now_mono = time.monotonic()
    done: list[str] = []
    changed = False

    for ticker, po in pending.items():
        if ticker in positions:
            log.info(f"  [po] {ticker} 이미 보유 중 → 미체결 주문 취소")
            _api.cancel_order(ticker, po.odno, po.qty)
            done.append(ticker)
            continue

        actual_qty = holdings_api.get(ticker, 0)
        if actual_qty > 0:
            if actual_qty < po.qty:
                log.warning(
                    f"  부분 체결: {ticker}  주문={po.qty}  체결={actual_qty} → 잔량 취소"
                )
                _api.cancel_order(ticker, po.odno, po.qty - actual_qty)

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
            _api.cancel_order(ticker, po.odno, po.qty)
            done.append(ticker)

    for t in done:
        del pending[t]

    return changed


def _cancel_all_pending(pending: dict[str, PendingOrder], _api: _kis.API):
    for ticker, po in pending.items():
        _api.cancel_order(ticker, po.odno, po.qty)
    pending.clear()


# ─────────────────────────────────────────
# 메인
# ─────────────────────────────────────────
def run():
    print()
    log.info(">>>>> START TRADER_V2 <<<<<<")
    today = dtutils.today()

    kh = query.get_krx_holiday(today)
    if kh is None:
        log.warning(f"krx_holiday에 {today} 데이터 없음. 영업일로 간주하고 계속 진행.")
    elif kh["opnd_yn"] != "Y":
        log.info(f"[{today}] 영업일이 아닙니다.")
        return

    try:
        _api = _kis.API(os.environ.get("KIS_ENV", "demo").lower(), FETCH_TIMEOUT)
    except RuntimeError as e:
        log.error(str(e))
        return

    messageutils.send_message(f"{today} trader를 시작합니다.")

    # ── 포지션 로드
    positions: dict[str, Position] = _load_positions()
    if positions:
        log.info(f"[포지션 로드] {len(positions)}종목")

    # ── 08:00 대기 (NXT 프리마켓 개장 — SOR 매수 가능)
    if dtutils.ctime() < NXT_PRE_OPEN:
        log.info(f"[대기] {NXT_PRE_OPEN[:2]}:00 NXT 프리마켓 대기…")
        while dtutils.ctime() < NXT_PRE_OPEN:
            time.sleep(5)

    # ── 상태 변수 (메인 루프 전 초기화 — pre po 처리에서 pending_po 필요)
    pending_po: dict[str, PendingOrder] = {}
    session_closed: dict[str, Position] = {}
    liquidated = False
    dirty = False
    tick_count = 0
    last_status_min = ""
    cur_prices: dict[str, float] = {}

    # ── PO① 전일 스캔 (pre po) 매수
    pre_po_name = get_pre_po_name()
    pre_po = PO_DIR / pre_po_name
    if pre_po.exists():
        log.info(
            f"[pre] {pre_po_name} 발견 → 전일 스캔 매수 ({PRE_MARKET_CASH_RATIO:.0%})"
        )
        raw = parse_po_file(pre_po)
        move_po_file(pre_po)
        if raw:
            pre_orders: dict[str, dict] = {}
            for o in raw:
                t = o.get("ticker")
                if t:
                    pre_orders[t] = o
            failed = _submit_buy_orders(
                pre_orders,
                positions,
                pending_po,
                _api,
                today,
                cash_usage=PRE_MARKET_CASH_RATIO,
                po_type="pre",
                excg_id_dvsn_cd="NXT",
            )
            if failed:
                log.info(f"  [pre] 주문실패 {len(failed)}종목 → KRX 개장 후 재시도")
    else:
        log.info(f"[pre] {pre_po_name} 없음")
        failed = {}

    retry_orders: dict[str, dict] = failed  # KRX 개장 후 재시도 대상
    retry_done = False

    # ── 기간 초과 포지션: KRX 개장 후 청산 (09:00 이전이면 메인 루프에서 처리)
    overdue_done = False
    if dtutils.ctime() >= KRX_OPEN:
        overdue = [
            t
            for t, p in list(positions.items())
            if p.expiry_date and today > p.expiry_date
        ]
        if overdue:
            log.info(f"[기간초과 청산] {len(overdue)}종목")
            for ticker in overdue:
                pos = positions[ticker]
                if _api.sell(ticker, pos.qty, f"기간초과 (expiry={pos.expiry_date})"):
                    _save_history(
                        "sell",
                        ticker,
                        pos.name,
                        pos.qty,
                        0,
                        "기간초과청산",
                        pos.po_type,
                    )
                    session_closed[ticker] = positions.pop(ticker)
                else:
                    log.warning(f"  기간초과 청산 실패: {ticker} → 다음 틱 재시도")
        overdue_done = True

    _save_positions(positions)
    log.info(f"[모니터링] {len(positions)}종목 감시 시작  (틱={TICK_SEC}s)")

    # ── 장 중 메인 루프
    while True:
        now = dtutils.ctime()
        is_slow_tick = tick_count % SLOW_EVERY == 0
        # [FIX] 포지션/pending 없으면 느린 틱 주기로 전환 (빈 10초 틱 방지)
        has_active = bool(positions) or bool(pending_po)

        if now >= MARKET_CLOSE:
            _cancel_all_pending(pending_po, _api)
            log.info("[종료] 장 마감")
            break

        # ── 0. KRX 개장 시: 기간초과 청산 + 프리마켓 실패 종목 재주문
        krx_open = now >= KRX_OPEN

        # 프리마켓 주문 실패 종목 재시도 (KRX 개장 후 1회)
        if krx_open and not retry_done and retry_orders:
            log.info(f"[KRX 개장] 프리마켓 실패 {len(retry_orders)}종목 재주문")
            still_failed = _submit_buy_orders(
                retry_orders,
                positions,
                pending_po,
                _api,
                today,
                cash_usage=PRE_MARKET_CASH_RATIO,
                po_type="pre",
            )
            if still_failed:
                log.warning(f"  재주문도 실패: {list(still_failed.keys())}")
            retry_done = True

        if krx_open and not overdue_done:
            overdue = [
                t
                for t, p in list(positions.items())
                if p.expiry_date and today > p.expiry_date
            ]
            if overdue:
                log.info(f"[KRX 개장] 기간초과 청산 {len(overdue)}종목")
                for ticker in overdue:
                    pos = positions[ticker]
                    if _api.sell(
                        ticker, pos.qty, f"기간초과 (expiry={pos.expiry_date})"
                    ):
                        _save_history(
                            "sell",
                            ticker,
                            pos.name,
                            pos.qty,
                            0,
                            "기간초과청산",
                            pos.po_type,
                        )
                        session_closed[ticker] = positions.pop(ticker)
                    else:
                        log.warning(f"  기간초과 청산 실패: {ticker} → 다음 틱 재시도")
                dirty = True
            overdue_done = True

        # ── 1. 현재가 조회 + SL/TP 처리 (매 틱 — KRX 개장 후에만)
        if krx_open and positions:
            cur_prices = _api.fetch_prices(list(positions.keys()))
            closed: list[str] = []

            for ticker, pos in list(positions.items()):
                cur = cur_prices.get(ticker)
                if cur is None:
                    continue
                ret_pct = (cur - pos.buy_price) / pos.buy_price * 100
                log.debug(
                    f"  {ticker} {pos.name[:12]:12s}  현재={cur:,.0f}"
                    f"  SL={pos.sl:,.0f}  TP1={pos.tp1:,.0f}  TP2={pos.tp2:,.0f}"
                    f"  {ret_pct:+.2f}%  {'[T1완료]' if pos.t1_done else ''}"
                )
                is_closed, is_changed = _process_position(_api, pos, cur)
                if is_closed:
                    closed.append(ticker)
                if is_changed:
                    dirty = True

            if closed:
                for ticker in closed:
                    session_closed[ticker] = positions.pop(ticker)

        # ── 2. po 파일 감시 + pending 체결 확인 (느린 틱)
        if is_slow_tick:
            if now < LIQUIDATE_TIME:
                orders = collect_po_orders(exclude_final=True)
                if orders:
                    log.info(f"[po] {len(orders)}종목 주문 발견")
                    _submit_buy_orders(
                        orders,
                        positions,
                        pending_po,
                        _api,
                        today,
                        cash_usage=MORNING_CASH_RATIO,
                        po_type="morning",
                    )

            if _check_pending_orders(pending_po, positions, _api, today):
                dirty = True

        # ── 3. 만기 청산 (1회)
        if not liquidated and now >= LIQUIDATE_TIME:
            log.info(f"만기 청산 시작")
            to_liq = [
                (t, p)
                for t, p in list(positions.items())
                if p.expiry_date and p.expiry_date <= today
            ]
            log.info(f"  만기 청산 대상: {len(to_liq)}종목")

            for ticker, pos in to_liq:
                reason = f"만기청산 (expiry={pos.expiry_date})"
                if _api.sell(ticker, pos.qty, reason):
                    _save_history(
                        "sell", ticker, pos.name, pos.qty, 0, reason, pos.po_type
                    )
                    session_closed[ticker] = positions.pop(ticker)
                    log.info(f"  청산: {ticker} {pos.name}  qty={pos.qty}")
                else:
                    log.warning(f"  청산 실패: {ticker} → 다음 영업일 재시도")

            # final po 처리
            final_po_name = get_final_po_name()
            po_final = PO_DIR / final_po_name
            if po_final.exists():
                log.info(f"{final_po_name} 처리")
                raw = parse_po_file(po_final)
                move_po_file(po_final)
                if raw:
                    after_orders: dict[str, dict] = {}
                    for o in raw:
                        t = o.get("ticker")
                        if t:
                            after_orders[t] = o
                    time.sleep(2)
                    _, _, cash = _api.get_balance()
                    cash_limit = cash * AFTERNOON_CASH_RATIO * CASH_USAGE
                    _submit_buy_orders(
                        after_orders,
                        positions,
                        pending_po,
                        _api,
                        today,
                        cash_limit=cash_limit,
                        po_type="final",
                    )
            else:
                log.info(f" {final_po_name} 없음")

            liquidated = True
            dirty = True

        # ── 4. 종료 조건
        if not positions and not pending_po and liquidated:
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
                    f"  체결대기 {len(pending_po)}종목: " + ", ".join(pending_po.keys())
                )

        # ── 7. 대기 (포지션 있으면 TICK_SEC, 없으면 SLOW_EVERY×TICK_SEC)
        tick_count += 1
        wait_sec = TICK_SEC if has_active else TICK_SEC * SLOW_EVERY
        deadline = time.monotonic() + wait_sec
        while time.monotonic() < deadline:
            if dtutils.ctime() >= MARKET_CLOSE:
                break
            time.sleep(1)

    # ── 당일 결과 요약
    if session_closed:
        total_pnl = sum(p.realized_pnl for p in session_closed.values())
        winners = sum(1 for p in session_closed.values() if p.realized_pnl > 0)
        message = (
            f"[당일 결과] 청산 {len(session_closed)}종목"
            f"  추정손익 {total_pnl:+,.0f}원"
            f"  수익 {winners}/손실 {len(session_closed) - winners}"
        )
        log.info(message)
        messageutils.send_message(message)

        for t, p in session_closed.items():
            log.info(f"  {t} {p.name}  {p.realized_pnl:+,.0f}원")

    # ── 종료: 스윙 포지션만 저장
    _save_positions(positions, swing_only=True)
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

    messageutils.send_message(f"{today} trader가 종료됩니다.")


# ─────────────────────────────────────────
# 진입점
# ─────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )
    kis_env = os.environ.get("KIS_ENV", "demo")
    if kis_env == "real":
        confirm = input(
            "🚨 실전투자(KIS_ENV=real) 모드입니다. 계속하시겠습니까? (yes): "
        )
        if confirm.strip().lower() != "yes":
            raise SystemExit(0)
    run()
