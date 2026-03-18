"""
자동 매매 트레이더 v20
─────────────────────────────────────────────────────
실행:
    uv run python -m blsh.wye.domestic.trader

환경변수:
    KIS_ENV=demo   모의투자 (기본)
    KIS_ENV=real   실전투자 🚨

매매 전략:
    1. 08:50 scanner.scan() → screen() 으로 투자 대상 선별
    2. 09:00 선별 종목 지정가(entry_price) 매수 (갭 상승 자동 스킵)
    3. 09:10 미체결 주문 취소
    4. 장 중 30초 간격 전 종목 현재가 병렬 조회 → ATR 기반 SL/TP 처리
       - 손절: 현재가 ≤ dynamic_sl → 전량 시장가 매도
       - 1차 익절(TP1 = buy+ATR×1.0): 50% 매도, SL → 매수가(본전 보장)
       - 2차 익절(TP2 = buy+ATR×ATR_TP_MULT): 잔여 전량 매도
       - 트레일링 SL: 주가 상승 시 SL을 (현재가 - ATR×ATR_SL_MULT) 로 상향
    5. 청산 조건
       - 데이 트레이딩(max_hold_days=0): 15:20 미청산 전량 매도
       - 스윙(max_hold_days>0): 보유일 초과 시 또는 다음 영업일 재실행 시 청산
         포지션은 ~/.blsh/config/trader_positions.json 에 영속 저장

수정 전략:
    1. 09:00 포지션 읽어와서 보유종목 모니터링.
        장 중 30초 간격 전 종목 현재가 병렬 조회 → ATR 기반 SL/TP 처리
        - 손절: 현재가 ≤ dynamic_sl → 전량 시장가 매도
        - 1차 익절(TP1 = buy+ATR×1.0): 50% 매도, SL → 매수가(본전 보장)
        - 2차 익절(TP2 = buy+ATR×ATR_TP_MULT): 잔여 전량 매도
        - 트레일링 SL: 주가 상승 시 SL을 (현재가 - ATR×ATR_SL_MULT) 로 상향
    2. 10:00 비동기 데이터 수집 및 투자 종목 선별. callback으로 선별 종목 지정가 매수. 
        기 보유종목은 매수 제외. 매수 성공시 보유종목 다시 조회.
    3. 10:10 미체결 주문 취소
    4. 15:20 비동기 데이터 수집 및 투자 종목 선별
    5. 15:20 비동기 청산
        청산 조건
        - 오늘이 청산일인 종목. 청산하지 못하면 다음 영업일 재실행 시 청산.
          포지션은 ~/.blsh/config/trader_positions.json 에 영속 저장
    6. 4와 5의 CALLBACK으로 잔고의 1/2 사용하여 선별 종목 시장가 매수.
    비고 : 매수, 매도 이벤트시 비동기로 DB 저장. 수익율은 나중에 계산.

데이/스윙 모드: 종목 모드별 _factor.py MAX_HOLD_DAYS 기준으로 자동 분류
    MOM → MAX_HOLD_DAYS_MOM, MIX → MAX_HOLD_DAYS_MIX, REV → MAX_HOLD_DAYS
    max_hold_days == 0 이면 데이 트레이딩
─────────────────────────────────────────────────────
"""

import json
import logging
import os
import time

from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path

from blsh.wye.domestic import _factor as fac
from blsh.wye.domestic._kis import _Api
from blsh.database import query
from blsh.common import dtutils
from blsh.wye.domestic._tick import floor_tick as _floor_tick, ceil_tick as _ceil_tick

log = logging.getLogger(__name__)

# ─────────────────────────────────────────
# 설정
# ─────────────────────────────────────────
CASH_USAGE = 0.9  # 가용 현금의 90% 사용
MIN_ALLOC = 10_000  # 종목당 최소 배분액 (1만원)
FILL_WAIT_UNTIL = "091000"  # 체결 대기 마감 (09:10)
DAYTRADE_CLOSE = "151500"  # 데이 트레이딩 강제 청산 시각 (30초 여유)
MARKET_CLOSE = "153000"  # 장 마감

TP1_MULT = 1.0  # 1차 익절: buy + ATR × TP1_MULT (50% 분할 매도)
SELL_COST_RATE = 0.002  # 증권거래세 + 수수료 합산 (약 0.2%)
POSITIONS_FILE = Path.home() / ".blsh" / "config" / "trader_positions.json"

POLL_SEC = 30  # 현재가 체크 주기 (초)
GAP_DOWN_LIMIT = 0.03  # 갭하락 하한: entry 대비 3% 이상 하락 시 스킵



# ─────────────────────────────────────────
# 포지션
# ─────────────────────────────────────────
@dataclass
class Position:
    ticker: str
    name: str
    qty: int  # 현재 잔여 수량
    buy_price: float  # 실제 체결가
    atr: float  # 진입 시 ATR
    atr_sl_mult: float  # 스캔 시점 ATR_SL_MULT (저장값)
    atr_tp_mult: float  # 스캔 시점 ATR_TP_MULT (저장값)
    sl: float  # 현재 동적 손절가 (트레일링)
    tp1: float  # 1차 목표가 (buy + ATR×TP1_MULT)
    tp2: float  # 2차 목표가 (buy + ATR×atr_tp_mult)
    mode: str
    max_hold_days: int
    entry_date: str  # YYYYMMDD
    expiry_date: str = ""  # 만기 거래일 (scanner에서 계산, YYYYMMDD)
    t1_done: bool = False  # 1차 분할 매도 완료 여부
    qty_t1: int = 0  # 1차 분할 수량 (전체의 50%)
    realized_pnl: float = 0.0  # 세션 내 누적 추정 실현손익 (매도가 × 수량 기준)


def _make_position(
    c: dict, buy_price: float, qty: int, entry_date: str, expiry_date: str = ""
) -> Position:
    """스캔 결과 dict → Position 생성. qty는 _buy 호출 시 사용한 수량과 일치해야 함."""
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
    )


# ─────────────────────────────────────────
# 포지션 영속화
# ─────────────────────────────────────────
def _load_positions() -> dict[str, Position]:
    if not POSITIONS_FILE.exists():
        return {}
    try:
        data = json.loads(POSITIONS_FILE.read_text())
        today = date.today().strftime("%Y%m%d")
        valid: dict[str, Position] = {}
        for t, v in data.items():
            v.setdefault("realized_pnl", 0.0)  # 구버전 파일 호환
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
    """포지션 파일 저장.
    swing_only=False(기본): 전체 저장 — 장 중 비정상 종료 시 복구용
    swing_only=True: 스윙 포지션만 저장 — 세션 종료 시 사용
    """
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


# ─────────────────────────────────────────
# SL/TP 처리 (단일 포지션)
# ─────────────────────────────────────────
def _sell_or_log(_api, ticker: str, qty: int, reason: str) -> bool:
    """매도 시도. 실패 시 CRITICAL 로그 후 False 반환 (다음 틱에서 재시도)."""
    if _api._sell(ticker, qty, reason):
        return True
    log.critical(f"  🚨 매도 실패: {ticker} [{reason}] → 다음 틱 재시도")
    return False


def _process_position(_api, pos: Position, current: float) -> bool:
    """
    현재가 기준으로 SL/TP 처리.
    포지션이 완전 청산되면 True 반환.
    """
    ret_pct = (current - pos.buy_price) / pos.buy_price * 100

    # ── 트레일링 SL 업데이트 (주가 상승 시에만 상향, 현재가 아래 유지)
    trail_sl = _floor_tick(current - pos.atr_sl_mult * pos.atr)
    if trail_sl > pos.sl and trail_sl < current:
        log.info(
            f"  🔺 트레일링 SL: {pos.ticker}  {pos.sl:,.0f} → {trail_sl:,.0f}"
            f"  (현재={current:,.0f})"
        )
        pos.sl = trail_sl

    # ── 손절
    if current <= pos.sl:
        qty_sell = pos.qty
        reason = f"손절 {ret_pct:+.2f}% (SL={pos.sl:,.0f})"
        if _sell_or_log(_api, pos.ticker, qty_sell, reason):
            pos.realized_pnl += (
                current - pos.buy_price
            ) * qty_sell - current * qty_sell * SELL_COST_RATE
            pos.qty = 0
        return pos.qty == 0

    # ── 1차 익절 (미완료 시)
    if not pos.t1_done and current >= pos.tp1:
        qty_sell = pos.qty_t1
        reason = f"1차익절 {ret_pct:+.2f}% (TP1={pos.tp1:,.0f})"
        if _sell_or_log(_api, pos.ticker, qty_sell, reason):
            pos.realized_pnl += (
                current - pos.buy_price
            ) * qty_sell - current * qty_sell * SELL_COST_RATE
            pos.qty -= qty_sell
            pos.t1_done = True
            # SL → 매수가 (본전 보장)
            if pos.buy_price > pos.sl:
                log.info(
                    f"  🔒 SL 본전 이동: {pos.ticker}  {pos.sl:,.0f} → {pos.buy_price:,.0f}"
                )
                pos.sl = pos.buy_price
        return pos.qty == 0

    # ── 2차 익절 (잔여 전량)
    if current >= pos.tp2:
        qty_sell = pos.qty
        reason = f"2차익절 {ret_pct:+.2f}% (TP2={pos.tp2:,.0f})"
        if _sell_or_log(_api, pos.ticker, qty_sell, reason):
            pos.realized_pnl += (
                current - pos.buy_price
            ) * qty_sell - current * qty_sell * SELL_COST_RATE
            pos.qty = 0
        return pos.qty == 0

    return False


# ─────────────────────────────────────────
# 메인
# ─────────────────────────────────────────
def _calc_expiry(pos: Position) -> str:
    """포지션의 만기 거래일(YYYYMMDD) 반환. 실패 시 빈 문자열."""
    try:
        dt = dtutils.add_biz_days(pos.entry_date, pos.max_hold_days)
        return dt if dt else ""
    except Exception as e:
        log.warning(f"만기일 계산 실패 ({pos.ticker}): {e} → 만기 미처리")
        return ""


def run():
    print()
    log.info(">>>>> START TRADER <<<<<<")
    today = dtutils.today()
    kh = query.get_krx_holiday(today)
    if kh is None:
        log.warning(
            f"krx_holiday에 {today} 데이터 없습니다. 영업일로 간주하고 계속 진행합니다."
        )
    elif kh["opnd_yn"] != "Y":
        log.info("영업일이 아닙니다.")
        return

    # ── 환경변수로 실전/모의 결정
    kis_env = os.environ.get("KIS_ENV", "demo").lower()
    try:
        _api = _Api(kis_env, POLL_SEC)
    except RuntimeError as e:
        log.error(str(e))
        return

    # ── 1. 스캔 (08:50 이후)
    if dtutils.now() < "085000":
        log.info("[대기] 08:50까지 대기 중…")
        while dtutils.now() < "085000":
            time.sleep(10)

    try:
        candidates = query.get_candidates(today) or []
        log.info(f"매수 대상 종목 {len(candidates)}건")
    except Exception as e:
        log.error(f"매수 대상 종목 조회 실패: {e}")
        return

    for c in candidates:
        log.info(
            f"  {c['ticker']:8s} {c.get('name', ''):18s}"
            f"  score={c['buy_score']}  mode={c['mode']}"
            f"  entry={c['entry_price']:,.0f}  ATR={c['atr']:.1f}"
            f"  SL={c['stop_loss']:,.0f}  TP={c['take_profit']:,.0f}"
        )

    # ── 2. 기존 스윙 포지션 로드 + 구버전 포지션 만기일 보정 (1회 DB 쿼리)
    positions: dict[str, Position] = _load_positions()
    for p in positions.values():
        if not p.expiry_date and p.max_hold_days > 0:
            p.expiry_date = _calc_expiry(p)
    if positions:
        log.info(f"[스윙 포지션] 기존 {len(positions)}종목 로드")

    # ── 3. 장 시작 대기
    if dtutils.now() < "090000":
        log.info("[대기] 09:00 장 시작 대기…")
        while dtutils.now() < "090000":
            time.sleep(5)

    # ── 4. 잔고·보유 종목 확인
    holdings_init, cash = _api._get_balance()
    held = set(holdings_init.keys())
    log.info(f"[잔고] 현금={cash:,.0f}원  보유={len(held)}종목")

    # ── 5. 매수 주문 (신규 + 갭 체크 후 배분)
    new_cands = [
        c
        for c in candidates
        if c["ticker"] not in held and c["ticker"] not in positions
    ]
    swing_tickers = set(
        positions.keys()
    )  # 파일에서 로드한 스윙 포지션 (체결확인 오탐 방지)
    pending: dict[str, dict] = {}  # ticker → {cand, odno, entry_price, qty}

    if new_cands:
        # Pass 1: 현재가 조회 → 갭 체크로 실제 매수 가능 종목 확정
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
            gap_down_floor = entry * (1 - GAP_DOWN_LIMIT)
            if cur < gap_down_floor:
                log.info(
                    f"  ⚠️  갭하락 스킵: {t}  현재={cur:,.0f} < entry×{1-GAP_DOWN_LIMIT:.0%}={gap_down_floor:,.0f}"
                )
                continue
            valid_cands.append((c, entry))

        # Pass 2: 유효 종목 수 기준으로 배분액 계산 후 매수
        if valid_cands:
            avail = cash * CASH_USAGE
            alloc = avail / len(valid_cands)
            if alloc < MIN_ALLOC:
                log.warning(
                    f"[매수] 배분액 {alloc:,.0f}원 < 최소 {MIN_ALLOC:,}원 → 전체 스킵"
                )
            else:
                for c, entry in valid_cands:
                    t = c["ticker"]
                    qty = max(1, int(alloc // entry))
                    odno = _api._buy(t, qty, entry)
                    if odno:
                        pending[t] = {
                            "cand": c,
                            "odno": odno,
                            "entry_price": entry,
                            "qty": qty,
                        }

    # ── 6. 체결 확인 (09:10까지) — inquire_balance로 실제 보유 여부 확인
    log.info(f"[체결대기] {FILL_WAIT_UNTIL[:2]}:{FILL_WAIT_UNTIL[2:4]}까지 대기")
    while dtutils.now() < FILL_WAIT_UNTIL and pending:
        holdings, _ = _api._get_balance()
        filled = []
        for t, info in pending.items():
            actual_qty = holdings.get(t, 0)
            if actual_qty > 0 and t not in swing_tickers:
                # 부분 체결: 잔여 주문 즉시 취소
                if actual_qty < info["qty"]:
                    log.warning(
                        f"  부분 체결: {t}  주문={info['qty']}  체결={actual_qty} → 잔량 취소"
                    )
                    if not _api._cancel_order(
                        t, info["odno"], info["qty"] - actual_qty
                    ):
                        log.error(f"  잔량 취소 실패 ({t}) → 포지션 수량 불일치 주의")
                pos = _make_position(
                    info["cand"],
                    info["entry_price"],
                    actual_qty,
                    today,
                    info["cand"].get("expiry_date") or "",
                )
                positions[t] = pos
                if pos.max_hold_days > 0 and not pos.expiry_date:
                    pos.expiry_date = _calc_expiry(pos)
                filled.append(t)
                log.info(
                    f"  ✅ 체결: {t} {pos.name}  매수가={info['entry_price']:,.0f}"
                    f"  SL={pos.sl:,.0f}  TP1={pos.tp1:,.0f}  TP2={pos.tp2:,.0f}"
                    f"  보유기간={pos.max_hold_days}일"
                )
        for t in filled:
            del pending[t]
        if pending:
            time.sleep(10)

    # 미체결 취소
    for t, info in list(pending.items()):
        _api._cancel_order(t, info["odno"], info["qty"])

    if not positions:
        log.info("[모니터링] 포지션 없음 → 종료")
        return

    _save_positions(positions)
    log.info(f"[모니터링] {len(positions)}종목 감시 시작")

    # ── 7. 장 중 모니터링
    last_status_min = ""
    session_closed: dict[str, Position] = {}  # 세션 내 청산된 포지션 누적 (손익 집계용)
    while True:
        now = dtutils.now()

        if now >= MARKET_CLOSE:
            log.info("[종료] 장 마감")
            break

        if not positions:
            log.info("[모니터링] 전 포지션 청산")
            break

        # 전 포지션 현재가 병렬 조회 (만기·데이 청산·SL/TP 공용 — API 호출 1회로 통합)
        cur_prices = _api._fetch_prices(list(positions.keys()))

        # 만기 청산 (스윙 보유일 초과)
        expired = [
            t
            for t, p in positions.items()
            if p.expiry_date and dtutils.today() > p.expiry_date
        ]
        if expired:
            for t in expired:
                if t not in positions:
                    continue
                pos = positions[t]
                cur = cur_prices.get(t)
                reason = (
                    f"보유만기 {(cur - pos.buy_price) / pos.buy_price * 100:+.2f}%"
                    if cur
                    else "보유만기 (가격조회실패)"
                )
                if _api._sell(t, pos.qty, reason=reason):
                    if cur:
                        pos.realized_pnl += (
                            cur - pos.buy_price
                        ) * pos.qty - cur * pos.qty * SELL_COST_RATE
                    session_closed[t] = pos
                    del positions[t]

        # 데이 트레이딩 강제 청산 (max_hold_days==0) — expired 처리 후 재구성
        if now >= DAYTRADE_CLOSE:
            day_tickers = [t for t, p in positions.items() if p.max_hold_days == 0]
            if day_tickers:
                log.info(f"[데이청산 진행 중] 잔여 {len(day_tickers)}종목")
                for t in day_tickers:
                    if t not in positions:
                        continue
                    pos = positions[t]
                    cur = cur_prices.get(t, pos.buy_price)
                    ret = (cur - pos.buy_price) / pos.buy_price * 100
                    if _api._sell(t, pos.qty, reason=f"데이마감 {ret:+.2f}%"):
                        # 잔여 수량(pos.qty)에 대한 손익만 계산 (1차 익절분은 _process_position에서 반영됨)
                        pos.realized_pnl += (
                            cur - pos.buy_price
                        ) * pos.qty - cur * pos.qty * SELL_COST_RATE
                        session_closed[t] = pos
                        del positions[t]

        # _sell 실패 시 포지션이 남아 다음 틱 SL/TP 루프에서 재시도됨 (의도된 fallback)
        closed = []
        for t, pos in positions.items():
            cur = cur_prices.get(t)
            if cur is None:
                continue
            ret_pct = (cur - pos.buy_price) / pos.buy_price * 100
            log.debug(
                f"  {t} {pos.name[:12]:12s}  "
                f"현재={cur:,.0f}  SL={pos.sl:,.0f}  TP1={pos.tp1:,.0f}  TP2={pos.tp2:,.0f}  "
                f"{ret_pct:+.2f}%  {'[T1완료]' if pos.t1_done else ''}"
            )
            if _process_position(_api, pos, cur):
                closed.append(t)

        for t in closed:
            session_closed[t] = positions[t]
            del positions[t]

        # 포지션 현황 로그 (1분마다)
        cur_min = now[2:4]
        if cur_min != last_status_min and int(cur_min) % 2 == 0:
            last_status_min = cur_min
            log.info(
                f"[{now[:2]}:{cur_min}] 보유 {len(positions)}종목: "
                + ", ".join(
                    f"{t}({p.qty}주 {((cur_prices[t] - p.buy_price) / p.buy_price * 100):+.1f}%)"
                    if t in cur_prices
                    else f"{t}({p.qty}주 -조회실패-)"
                    for t, p in positions.items()
                )
            )

        _save_positions(positions, swing_only=False)
        deadline = time.monotonic() + POLL_SEC
        while time.monotonic() < deadline:
            if dtutils.now() >= MARKET_CLOSE:
                break
            time.sleep(1)

    # ── 8. 당일 결과 요약
    if session_closed:
        total_pnl = sum(p.realized_pnl for p in session_closed.values())
        winners = sum(1 for p in session_closed.values() if p.realized_pnl > 0)
        log.info(
            f"[당일 결과] 청산 {len(session_closed)}종목  "
            f"추정손익 {total_pnl:+,.0f}원  수익 {winners}/손실 {len(session_closed) - winners}"
        )
        for t, p in session_closed.items():
            log.info(f"  {t} {p.name}  {p.realized_pnl:+,.0f}원")

    # ── 9. 종료 처리 (스윙만 파일에 남김)
    _save_positions(positions, swing_only=True)
    swing_remaining = {t: p for t, p in positions.items() if p.max_hold_days > 0}
    log.info(
        f"[세션 종료]  스윙 잔여={len(swing_remaining)}종목"
        + (f"  → {POSITIONS_FILE}" if swing_remaining else "")
    )
    for t, p in swing_remaining.items():
        log.info(
            f"  {t} {p.name}  qty={p.qty}  SL={p.sl:,.0f}  TP2={p.tp2:,.0f}  만기={p.expiry_date}"
        )


# ─────────────────────────────────────────
# 진입점
# ─────────────────────────────────────────
if __name__ == "__main__":
    # logging.basicConfig(
    #     level=logging.INFO,
    #     format="%(asctime)s %(levelname)s %(message)s",
    #     datefmt="%H:%M:%S",
    # )

    kis_env = os.environ.get("KIS_ENV", "demo")
    if kis_env == "real":
        confirm = input(
            "🚨 실전투자(KIS_ENV=real) 모드입니다. 계속하시겠습니까? (yes): "
        )
        if confirm.strip().lower() != "yes":
            raise SystemExit(0)

    run()
