"""
백테스트 시뮬레이터

- entry_date OHLCV 포함 (매수일 누락 수정)
- buy_price 기준 SL/TP 재계산 (scanner 종가 기준 → 실제 매수가 기준)
- TP1 분할매도(factor.TP1_MULT/TP1_RATIO) + 트레일링 SL 반영 (trader 로직 일치)
- 모드별 max_hold_days 지원 (DAY max_hold=0 당일청산 포함)
- 보수적 트레일링: 전일 high 기준으로만 SL 갱신 (일봉 한계 감안)
"""

import logging
import pandas as pd
from wye.blsh.database import query
from wye.blsh.domestic import reporter, Tick, factor
from wye.blsh.domestic._sim_core import sim_one, SELL_COST_RATE
from wye.blsh.domestic.trader import MIN_ALLOC, CASH_USAGE

log = logging.getLogger(__name__)


def simulate(candidates, cash: float = 0) -> tuple | None:
    """
    수익률 시뮬레이트.

    Args:
        candidates: find_candidates() 반환 DataFrame
        cash: 초기 잔고 (0이면 수익률만 계산, >0이면 trader 배분 로직으로 손익금액 계산)

    Returns:
        (rows_ok, rows_gap, rows_miss) 또는 데이터 없으면 None
    """
    if candidates.empty:
        log.info("[시뮬레이트] 후보 종목 없음")
        return None

    entry_date = candidates.iloc[0]["entry_date"]
    max_hold = factor.MAX_HOLD_DAYS

    log.info(f"[시뮬레이트] 매수일({entry_date}) 이후 최대 {max_hold}거래일 추적")

    tickers = candidates["ticker"].tolist()

    # ── entry_date 포함 + 이후 MAX_HOLD_DAYS 거래일 날짜 목록 조회
    # [FIX] entry_date 자체를 포함해야 당일 매수 시뮬레이션 가능
    date_rows_after = pd.DataFrame(
        query.get_max_hold_dates(entry_date, max(max_hold, 1))
    )
    # entry_date 자체 + 이후 날짜
    hold_dates = [entry_date] + (
        date_rows_after["d"].tolist() if not date_rows_after.empty else []
    )

    # entry_date에 OHLCV 데이터가 있는지 확인
    if not query.has_ohlcv_data(entry_date):
        log.info(f"[시뮬레이트] {entry_date} OHLCV 데이터 없음 → 스킵")
        return None

    actual_days = len(hold_dates)
    log.info(f"  확인 기간: {hold_dates[0]} ~ {hold_dates[-1]}  ({actual_days}거래일)")

    def fetch_ohlcv_range(table):
        try:
            return pd.DataFrame(query.get_ohlcv_range(table, hold_dates, tickers))
        except Exception as e:
            log.warning(f"  OHLCV 조회 오류 ({table}): {e}")
            return pd.DataFrame()

    ohlcv_all = pd.concat(
        [fetch_ohlcv_range("isu_ksp_ohlcv"), fetch_ohlcv_range("isu_ksd_ohlcv")],
        ignore_index=True,
    )

    # ticker → {trd_dd: {open, high, low, close}} 인덱스 구성
    ohlcv_idx: dict[str, dict[str, dict]] = {}
    for _, row in ohlcv_all.iterrows():
        ohlcv_idx.setdefault(row["ticker"], {})[row["trd_dd"]] = {
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
        }

    # ── trader 배분 로직 (cash > 0 일 때)
    if cash > 0:
        avail = cash * CASH_USAGE
        alloc = avail / len(candidates)
        if alloc < MIN_ALLOC:
            log.warning(
                f"[시뮬레이트] 배분액 {alloc:,.0f}원 < 최소 {MIN_ALLOC:,}원 → 최소액으로 진행"
            )
            alloc = MIN_ALLOC
    else:
        alloc = 0

    rows_ok = []
    rows_gap = []
    rows_miss = []

    for _, sig in candidates.iterrows():
        t = sig["ticker"]
        entry = float(sig["entry_price"])
        atr = float(sig["atr"])
        atr_sl_mult = float(sig.get("atr_sl_mult", factor.ATR_SL_MULT))
        atr_tp_mult = float(sig.get("atr_tp_mult", factor.ATR_TP_MULT))
        days = ohlcv_idx.get(t, {})

        # entry_date(hold_dates[0]) 데이터 없음
        t1_ohv = days.get(hold_dates[0])
        if t1_ohv is None:
            rows_miss.append(sig.to_dict())
            continue

        # 갭 상승 체크
        if t1_ohv["open"] > entry:
            rows_gap.append(
                {**sig.to_dict(), "t_open": t1_ohv["open"], "entry_date": hold_dates[0]}
            )
            continue

        # [FIX] 실제 매수가 기준 SL/TP 재계산 (trader _make_position과 동일)
        buy_price = t1_ohv["open"]
        sl = Tick.floor_tick(buy_price - atr_sl_mult * atr)
        tp1 = Tick.ceil_tick(buy_price + factor.TP1_MULT * atr)
        tp2 = Tick.ceil_tick(buy_price + atr_tp_mult * atr)

        # 모드별 최대 보유 기간
        mode = sig.get("mode", "")
        if mode == "MOM":
            max_days = factor.MAX_HOLD_DAYS_MOM
        elif mode == "MIX":
            max_days = factor.MAX_HOLD_DAYS_MIX
        else:
            max_days = factor.MAX_HOLD_DAYS

        # DAY 모드(max_days=0): entry_date 당일만 보유
        if max_days == 0:
            sig_hold_dates = [hold_dates[0]]
        else:
            sig_hold_dates = hold_dates[: max_days + 1]

        result_type, ret_pct, exit_price, exit_date, last_ohv = sim_one(
            buy=buy_price,
            sl=sl,
            tp1=tp1,
            tp2=tp2,
            tp1_ratio=factor.TP1_RATIO,
            atr_sl_mult=atr_sl_mult,
            atr=atr,
            dates=sig_hold_dates,
            get_ohv=days.get,
        )

        # "미확정" 레이블에 보유일수 추가
        if result_type == "미확정":
            if max_days == 0:
                result_type = "데이청산"
            else:
                result_type = f"미확정({len(sig_hold_dates)}일)"

        last_ohv = last_ohv or t1_ohv
        qty = max(1, int(alloc // buy_price)) if alloc > 0 else None
        pnl_per_unit = ret_pct / 100 * buy_price
        pnl_amount = pnl_per_unit * qty if qty is not None else None
        rows_ok.append(
            {
                **sig.to_dict(),
                "buy_price": buy_price,
                "entry_date": hold_dates[0],
                "exit_price": exit_price,
                "exit_date": exit_date,
                "result_type": result_type,
                "ret_pct": ret_pct,
                "qty": qty,
                "pnl_amount": pnl_amount,
                "t_open": t1_ohv["open"],
                "t_high": last_ohv["high"],
                "t_low": last_ohv["low"],
                "t_close": last_ohv["close"],
            }
        )

    reporter.print_simul_report(
        entry_date,
        candidates,
        rows_ok,
        rows_gap,
        rows_miss,
        cash=cash,
    )

    return rows_ok, rows_gap, rows_miss


if __name__ == "__main__":
    import sys
    from wye.blsh.common import dtutils
    from wye.blsh.domestic import scanner

    dt = (
        sys.argv[1]
        if len(sys.argv) > 1
        else dtutils.prev_biz_date(dtutils.max_ohlcv_date())
    )
    ca = float(sys.argv[2]) if len(sys.argv) > 2 else 10_000_000

    simulate(scanner.find_candidates(dt), cash=ca)

    # from wye.blsh.domestic.codex import scanner_codex

    # simulate(scanner_codex.find_candidates(dt), cash=ca)
