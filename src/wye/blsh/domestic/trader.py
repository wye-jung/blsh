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
    1. 08:00 NXT 프리마켓 개장 → PO①(전일스캔) NXT 지정가 매수 (30%).
       NXT 비대상 종목 → KRX 개장 후 재시도.
       09:00 KRX 정규장 개장 → SL/TP 매도 + 기간초과 청산 시작.
       보유종목 모니터링.
        장 중 현재가 조회 → ATR 기반 SL/TP 처리
        - 손절: 현재가 ≤ SL → 전량 시장가 매도
        - 1차 익절: TP1 = buy + ATR × TP1_MULT → TP1_RATIO 비율 매도, SL → 매수가(본전)
        - 2차 익절: TP2 = buy + ATR × ATR_TP_MULT → 잔여 전량 매도
        - 트레일링 SL: 주가 상승 시 SL을 (현재가 - ATR × ATR_SL_MULT) 로 상향
    2. ~10:10 PO②(오전 스캔) 감지 시 잔고의 15% 지정가 매수
        기 보유종목은 매수 제외. 10분 후 미체결 취소.
        PO 파일 처리 후 done 폴더로 이동.
    3. 15:15 만기 청산
        청산일(expiry_date) 도래 종목 전량 시장가 매도.
        청산 실패 시 다음 영업일 재시도 (포지션 영속 저장).
    4. 청산 직후 PO③(오후 스캔) 지정가 매수 (55% × 90%)
    5. 매수/매도 성공 시 trade_history DB + 텔레그램 알림
    6. 15:30 KRX 마감 → NXT 에프터마켓(~20:00) SL/TP 모니터링 지속
       NXT는 시장가 불가 → 지정가 매도 (Tick.floor_tick(현재가))

구조:
    완전 단일 스레드 — 작업별 차등 주기
    ┌──────────────────────────────────────────────────────┐
    │ 09:00~15:30 (KRX 정규장):                            │
    │   매 틱 (10초): 현재가 조회 + SL/TP (KRX 시장가 매도) │
    │   매 SLOW 틱 (30초): PO 감시 + 체결 확인             │
    │   15:15 만기 청산 + PO③ 매수                       │
    ├──────────────────────────────────────────────────────┤
    │ 15:30~20:00 (NXT 에프터마켓):                        │
    │   매 틱 (10초): 현재가 조회 + SL/TP (NXT 지정가 매도) │
    │   PO 감시 중단, 신규 매수 없음                      │
    └──────────────────────────────────────────────────────┘

PO 파일 포맷: ~/.blsh/{KIS_ENV}/data/po/po-{entry_date}-{po_type}.json
    po_type: pre (전일스캔), ini (장초매수), fin (청산후매수)
    내용: {ticker: {atr, atr_sl_mult, atr_tp_mult, tp1_mult, tp1_ratio,
                    entry_price, max_hold_days, mode, ...}}
─────────────────────────────────────────────────────
"""

import json
import logging
from logging.handlers import TimedRotatingFileHandler
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
    TP1_MULT,
    TP1_RATIO,
    MAX_HOLD_DAYS,
    CASH_USAGE,
    MIN_ALLOC,
    SELL_COST_RATE,
    PRE_CASH_RATIO,
    INI_CASH_RATIO,
    FIN_CASH_RATIO,
)
from wye.blsh.domestic.kis_client import KISClient
from wye.blsh.domestic.ws_monitor import PriceMonitor
from wye.blsh.common import dtutils, fileutils, messageutils
from wye.blsh.common.env import DATA_DIR, LOG_DIR, BACKUP_DIR, KIS_ENV, USE_WEBSOCKET
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
            log.critical(f"🚨 이력 저장 실패 [{KIS_ENV}] ({ticker}): {e}")
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
) -> bool:
    """KRX 시장가 매도 + 체결가 조회 후 이력 저장. 성공 시 True."""
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
        msg = f"🚨 매도 3회 실패: {name}({ticker}) [{reason}]"
        log.critical(msg)
        messageutils.send_message(msg)
        return False
    fill_price = None
    for wait in (2, 3, 5):  # 최대 3회: 2초 → 3초 → 5초 대기 후 조회
        time.sleep(wait)
        fill_price = kis.get_filled_price(ticker, odno, today)
        if fill_price is not None:
            break
        log.debug(f"  체결가 조회 대기 중: {ticker} odno={odno} (대기={wait}s)")
    if fill_price is None:
        log.warning(
            f"  ⚠️ 체결가 조회 실패: {name}({ticker}) odno={odno} → 현재가로 대체"
        )
        fill_price = kis.get_price(ticker)  # 최후 수단: 현재가 사용
    _save_history("sell", ticker, name, qty, fill_price, reason, po_type)
    return True


_SELL_FAIL_BALANCE_CHECK = 3  # 연속 N회 매도 실패 시 KIS 잔고 재확인


def _sell_or_log(
    kis: KISClient,
    pos: Position,
    qty: int,
    reason: str,
    nxt_price: int = 0,
    today: str = "",
) -> bool:
    """nxt_price > 0 이면 NXT 지정가 매도, 아니면 KRX 시장가 매도."""
    if nxt_price > 0:
        ok = kis.sell_nxt(pos.ticker, qty, nxt_price, reason)
        if ok:
            pos.sell_fail_count = 0
            _save_history(
                "sell", pos.ticker, pos.name, qty, nxt_price,
                f"{reason} (지정가. 실제 체결가 미정)", pos.po_type,
            )
            return True
        pos.sell_fail_count += 1
        log.critical(f"  🚨 매도 실패: {pos.ticker} [{reason}] → 다음 틱 재시도")
        # 연속 실패 시 KIS 잔고 확인 → 유령 포지션이면 True 반환하여 포지션 제거
        if pos.sell_fail_count >= _SELL_FAIL_BALANCE_CHECK:
            holdings, _, _ = kis.get_balance()
            if pos.ticker not in holdings or holdings[pos.ticker] <= 0:
                log.warning(
                    f"  ⚠️ 유령 포지션 감지: {pos.ticker} KIS 잔고 없음 → 포지션 제거"
                )
                messageutils.send_message(
                    f"⚠️ 유령 포지션 제거: {pos.ticker} [{reason}]"
                    f" (매도 {pos.sell_fail_count}회 실패, KIS 잔고 없음)"
                )
                pos.sell_fail_count = 0
                return True
            pos.sell_fail_count = 0
        return False
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
            log.warning(f"positions.json 없음 → .bak에서 복원 시도")
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
            v.pop("sell_fail_count", None)  # 세션 내 임시 값 — 이월 방지
            p = Position(**v)
            # [FIX] expiry_date 미설정 보정 (데이: entry_date, 스윙: +N영업일)
            if not p.expiry_date:
                try:
                    p.expiry_date = dtutils.add_biz_days(p.entry_date, p.max_hold_days)
                except Exception as e:
                    log.warning(f"  expiry_date 보정 실패 ({t}): {e}")
                    p.expiry_date = None
                if p.expiry_date:
                    log.info(
                        f"  expiry_date 보정: {t}  entry={p.entry_date}"
                        f"  +{p.max_hold_days}d → {p.expiry_date}"
                    )
                else:
                    msg = f"⚠️ expiry_date 보정 실패 ({t}) → 오늘 청산"
                    log.warning(msg)
                    messageutils.send_message(msg)
                    p.expiry_date = today  # 안전 fallback: 오늘 청산 대상
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

        sl = Tick.floor_tick(buy_price - atr_sl_mult * atr)
        tp1 = Tick.ceil_tick(buy_price + tp1_mult * atr)
        tp2 = Tick.ceil_tick(buy_price + atr_tp_mult * atr)
        qty_t1 = max(1, int(qty * tp1_ratio))

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
        log.info(
            f"  [복원] ✅ {ticker} {name}  매수가={buy_price:,.0f}  ATR={atr:.0f}"
            f"  SL={sl:,.0f}  TP1={tp1:,.0f}  TP2={tp2:,.0f}"
            f"  entry={entry_date}  t1_done={t1_done}"
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
    sl = Tick.floor_tick(buy_price - atr_sl_mult * atr)
    tp1 = Tick.ceil_tick(buy_price + tp1_mult * atr)
    tp2 = Tick.ceil_tick(buy_price + atr_tp_mult * atr)
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
    # NXT 모드: 현재가를 지정가로 사용 (0이면 KRX 시장가)
    sell_price = Tick.floor_tick(current) if nxt_mode else 0

    trail_sl = Tick.floor_tick(current - pos.atr_sl_mult * pos.atr)
    if trail_sl > pos.sl:
        if trail_sl < current:
            log.info(
                f"  🔺 트레일링 SL: {pos.ticker}  {pos.sl:,.0f} → {trail_sl:,.0f}"
                f"  (현재={current:,.0f})"
            )
            pos.sl = trail_sl
            changed = True
        else:
            # trail_sl ≥ current → SL이 현재가 이상이면 갱신 무의미 (즉시 손절 영역)
            log.warning(
                f"  ⚠️ 트레일링 SL 스킵: {pos.ticker}  trail_sl={trail_sl:,.0f}"
                f" ≥ 현재가={current:,.0f}  (ATR={pos.atr:.0f}, mult={pos.atr_sl_mult})"
            )

    # [FIX] 매도 성공 시에만 changed=True (실패 시 상태 불변)
    if current <= pos.sl:
        # NXT 손절: 거래소 하한가로 지정가 매도 (사실상 시장가 효과)
        if nxt_mode:
            detail = kis.get_price_detail(pos.ticker)
            sl_sell_price = Tick.floor_tick(detail[1]) if detail and detail[1] else Tick.floor_tick(current)
        else:
            sl_sell_price = 0  # KRX 시장가
        reason = f"손절 {ret_pct:+.2f}% (SL={pos.sl:,.0f})"
        if _sell_or_log(kis, pos, pos.qty, reason, nxt_price=sl_sell_price, today=today):
            pos.realized_pnl += (
                current - pos.buy_price
            ) * pos.qty - current * pos.qty * SELL_COST_RATE
            pos.qty = 0
            return True, True
        return False, changed

    if not pos.t1_done and current >= pos.tp1:
        qty_sell = min(pos.qty_t1, pos.qty)  # DB 복원 포지션 qty 불일치 방어
        reason = f"1차익절 {ret_pct:+.2f}% (TP1={pos.tp1:,.0f})"
        if _sell_or_log(kis, pos, qty_sell, reason, nxt_price=sell_price, today=today):
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
        if _sell_or_log(kis, pos, pos.qty, reason, nxt_price=sell_price, today=today):
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
    kis: KISClient,
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
    holdings_api, avg_prices, cash = kis.get_balance()
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
        qty = int(alloc // entry_price)
        if qty < 1:
            log.warning(
                f"[po] 배분액 부족: {ticker}  alloc={alloc:,.0f} < price={entry_price:,.0f} → 스킵"
            )
            continue
        odno = kis.buy(ticker, qty, entry_price, excg_id_dvsn_cd)
        if odno:
            pending[ticker] = PendingOrder(
                cand=o,
                odno=odno,
                entry_price=entry_price,
                qty=qty,
                deadline=deadline,
                po_type=po_type,
                excg_cd=excg_id_dvsn_cd,
            )
        else:
            failed[ticker] = o
            log.warning(f"  [po] 주문 실패: {ticker} → KRX 개장 후 재시도 대상")

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
        if ticker in positions:
            log.info(f"  [po] {ticker} 이미 보유 중 → 미체결 주문 취소")
            if not kis.cancel_order(ticker, po.odno, po.qty, po.excg_cd):
                log.warning(f"  [po] {ticker} 이미보유 취소 실패 (무시)")
            done.append(ticker)
            continue

        actual_qty = holdings_api.get(ticker, 0)
        if actual_qty > 0:
            if actual_qty < po.qty:
                log.warning(
                    f"  부분 체결: {ticker}  주문={po.qty}  체결={actual_qty} → 잔량 취소"
                )
                if not kis.cancel_order(
                    ticker, po.odno, po.qty - actual_qty, po.excg_cd
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
            if not kis.cancel_order(ticker, po.odno, po.qty, po.excg_cd):
                log.warning(f"  [po] 미체결 취소 실패: {ticker} odno={po.odno}")
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
    log.info(">>>>> START TRADER <<<<<<")
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

    mode_label = "🚨 실전투자" if KIS_ENV == "real" else "📋 모의투자"
    messageutils.send_message(f"[{today}] 트레이더 시작 ({mode_label})")

    try:
        kis = KISClient(KIS_ENV, FETCH_TIMEOUT)
    except RuntimeError as e:
        log.error(str(e))
        return

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
            today,
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
                    log.info(f"[KRX 마감] FIN PO 미체결 {len(fin_retry)}종목 → NXT 재발주")
                    _, _, cash = kis.get_balance()
                    cash_limit = cash * FIN_CASH_RATIO * CASH_USAGE
                    _submit_buy_orders(
                        fin_retry,
                        positions,
                        pending_po,
                        kis,
                        today,
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

            # ── 0. KRX 개장 시: 기간초과 청산 + 프리마켓 실패 종목 재주문
            krx_open = now >= Milestone.KRX_OPEN_TIME and not krx_closed

            # 프리마켓 주문 실패 종목 재시도 (KRX 개장 후 1회)
            if krx_open and not retry_done and retry_orders:
                log.info(f"[KRX 개장] 프리마켓 실패 {len(retry_orders)}종목 재주문")
                still_failed = _submit_buy_orders(
                    retry_orders,
                    positions,
                    pending_po,
                    kis,
                    today,
                    cash_usage=PRE_CASH_RATIO,
                    po_type=PO_TYPE_PRE,
                )
                if still_failed:
                    log.warning(f"  재주문도 실패: {list(still_failed.keys())}")
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
                    t for t in positions
                    if t not in holdings_chk or holdings_chk[t] <= 0
                }
                if ghost:
                    log.warning(
                        f"[유령 포지션] KIS 잔고에 없는 종목 {len(ghost)}건 제거:"
                        f" {list(ghost)}"
                    )
                    messageutils.send_message(
                        f"⚠️ [{today}] 유령 포지션 {len(ghost)}건 제거:"
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
                        f"[추적불가] positions에 없는 보유종목 {len(orphans)}건 발견"
                        f" → DB 복원 시도"
                    )
                    msg = (
                        f"⚠️ [{today}] 추적불가 종목 {len(orphans)}건 감지"
                        f" → DB 복원 시도: {list(orphans.keys())}"
                    )
                    messageutils.send_message(msg)
                    restored = _restore_positions_from_db(
                        orphans, avg_prices_chk, today
                    )
                    for ticker, pos in restored.items():
                        positions[ticker] = pos
                        dirty = True
                    if restored:
                        monitor.sync_subscriptions(list(positions.keys()))
                        messageutils.send_message(
                            f"✅ [{today}] {len(restored)}건 복원 성공:"
                            f" {list(restored.keys())}"
                        )
                    unrestorable = {
                        t: q for t, q in orphans.items() if t not in restored
                    }
                    if unrestorable:
                        log.warning(
                            f"[추적불가 청산] 복원 실패 {len(unrestorable)}건 → 시장가 청산"
                        )
                        for ticker, qty in unrestorable.items():
                            reason = "추적불가(복원실패)"
                            if _sell_market(kis, ticker, ticker, qty, reason, today):
                                log.info(f"  🚨 추적불가 청산: {ticker}  수량={qty}")
                            else:
                                log.warning(
                                    f"  추적불가 청산 실패: {ticker}  수량={qty}"
                                )

            # ── 0-2. 기간초과 청산 (유령 체크 후 실행, 슬로우 틱 간격 재시도)
            if krx_open and not overdue_done and is_slow_tick:
                overdue = [
                    t
                    for t, p in list(positions.items())
                    if p.expiry_date and today > p.expiry_date
                ]
                if overdue:
                    log.info(f"[기간초과 청산] {len(overdue)}종목 시도")
                    for ticker in overdue:
                        pos = positions[ticker]
                        if _sell_market(
                            kis,
                            ticker,
                            pos.name,
                            pos.qty,
                            "기간초과청산",
                            today,
                            pos.po_type,
                        ):
                            session_closed[ticker] = positions.pop(ticker)
                        else:
                            log.warning(
                                f"  기간초과 청산 실패: {ticker} → 30초 후 재시도"
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

            # ── 2. po 파일 감시 + pending 체결 확인 (느린 틱)
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
                                today,
                                cash_usage=INI_CASH_RATIO,
                                po_type=PO_TYPE_INI,
                            )
                            ini_po_bought = True

                # KRX + NXT: pending 체결 확인
                if pending_po and _check_pending_orders(pending_po, positions, kis, today):
                    dirty = True
                    monitor.sync_subscriptions(list(positions.keys()))

            # ── 3. 만기 청산 (1회)
            if not liquidated and now >= Milestone.LIQUIDATE_TIME:
                log.info(f"만기 청산 시작")
                to_liq = [
                    (t, p)
                    for t, p in list(positions.items())
                    if p.expiry_date and p.expiry_date <= today
                ]
                log.info(f"  만기 청산 대상: {len(to_liq)}종목")

                for ticker, pos in to_liq:
                    reason = f"만기청산 (expiry={pos.expiry_date})"
                    if _sell_market(
                        kis, ticker, pos.name, pos.qty, reason, today, pos.po_type
                    ):
                        session_closed[ticker] = positions.pop(ticker)
                        log.info(f"  청산: {ticker} {pos.name}  qty={pos.qty}")
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
                        today,
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
        # ── 웹소켓 종료
        monitor.stop()

        # ── 당일 결과 요약 (실제 체결가로 PnL 보정)
        if session_closed:
            sell_fills = kis.get_sell_fills(today)
            if sell_fills:
                # DB 매도 이력 체결가 보정 (NXT 지정가 → 실제 체결가)
                try:
                    query.update_sell_prices(today, sell_fills)
                except Exception as e:
                    log.warning(f"DB 매도 체결가 보정 실패: {e}")
            for t, p in session_closed.items():
                if t in sell_fills:
                    avg_price, total_qty = sell_fills[t]
                    old_pnl = p.realized_pnl
                    p.realized_pnl = (
                        (avg_price - p.buy_price) * total_qty
                        - avg_price * total_qty * SELL_COST_RATE
                    )
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
