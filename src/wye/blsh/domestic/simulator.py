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
from wye.blsh.domestic.trader import SELL_COST_RATE

log = logging.getLogger(__name__)


def simulate(candidates) -> tuple | None:
    """
    수익률 시뮬레이트.
    Returns: (rows_ok, rows_gap, rows_miss) 또는 데이터 없으면 None
    """
    if candidates.empty:
        log.info("[시뮬레이트] 후보 종목 없음")
        return None

    entry_date = candidates.iloc[0]["entry_date"]
    max_hold = factor.MAX_HOLD_DAYS

    log.info(f"[시뮬레이트] 목표일={entry_date}  최대 {max_hold}거래일 추적")

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

        result_type = None
        exit_price = None
        exit_date = None
        last_ohv = t1_ohv
        realized_pnl = 0.0
        remaining_qty = 1.0  # 비율 (1.0 = 전량)
        t1_done = False

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
            # entry_date(매수일) + 이후 max_days일
            sig_hold_dates = hold_dates[: max_days + 1]

        # 날짜 순서대로 SL/TP1/TP2 + 트레일링 SL 확인
        prev_high = t1_ohv["high"]  # 트레일링 SL용 전일 고가
        for d in sig_hold_dates:
            ohv = days.get(d)
            if ohv is None:
                continue
            last_ohv = ohv

            # [FIX] 트레일링 SL: 전일까지의 high 기준으로만 갱신 (보수적)
            # 일봉에서는 당일 high/low 선후를 알 수 없으므로,
            # 당일 high로 SL을 올린 뒤 당일 low로 손절 체크하면 낙관적 편향 발생.
            # 전일 high 기준 갱신 → 당일 low 체크 순서로 보수적 시뮬레이션.
            if d != sig_hold_dates[0]:  # 매수일은 전일 고가 = 매수일 자체
                trail_sl = Tick.floor_tick(prev_high - atr_sl_mult * atr)
                if trail_sl > sl and trail_sl < prev_high:
                    sl = trail_sl

            # 손절 체크
            if ohv["low"] <= sl:
                pnl = (
                    sl - buy_price
                ) * remaining_qty - sl * remaining_qty * SELL_COST_RATE
                realized_pnl += pnl
                result_type = "손절"
                exit_price = sl
                exit_date = d
                remaining_qty = 0
                break

            # TODO: TP1 체결 + 같은 봉 본전 SL 도달 시나리오 (일봉 한계)
            # TP1 분할매도 (50%) — 미완료 시에만
            if not t1_done and ohv["high"] >= tp1:
                sell_ratio = min(factor.TP1_RATIO, remaining_qty)
                pnl = (tp1 - buy_price) * sell_ratio - tp1 * sell_ratio * SELL_COST_RATE
                realized_pnl += pnl
                remaining_qty -= sell_ratio
                t1_done = True
                # SL → 매수가 (본전 보장)
                if buy_price > sl:
                    sl = buy_price

            # 트레일링 SL 갱신용 전일 고가 업데이트
            prev_high = max(prev_high, ohv["high"])

            # TP2 잔량 전량 청산
            if ohv["high"] >= tp2 and remaining_qty > 0:
                pnl = (
                    tp2 - buy_price
                ) * remaining_qty - tp2 * remaining_qty * SELL_COST_RATE
                realized_pnl += pnl
                result_type = "익절" if t1_done else "익절(전량)"
                exit_price = tp2
                exit_date = d
                remaining_qty = 0
                break

        # 최대 보유기간 후 미확정 → 마지막 거래일 종가 청산
        if result_type is None:
            close_price = last_ohv["close"]
            pnl = (
                close_price - buy_price
            ) * remaining_qty - close_price * remaining_qty * SELL_COST_RATE
            realized_pnl += pnl
            day_label = len(sig_hold_dates)
            if max_days == 0:
                result_type = "데이청산"
            else:
                result_type = f"미확정({day_label}일)"
            exit_price = close_price
            exit_date = sig_hold_dates[-1] if sig_hold_dates else hold_dates[0]
            remaining_qty = 0

        ret_pct = (realized_pnl / buy_price) * 100 if buy_price else 0
        rows_ok.append(
            {
                **sig.to_dict(),
                "buy_price": buy_price,
                "entry_date": hold_dates[0],
                "exit_price": exit_price,
                "exit_date": exit_date,
                "result_type": result_type,
                "ret_pct": ret_pct,
                "t_open": t1_ohv["open"],
                "t_high": last_ohv["high"],
                "t_low": last_ohv["low"],
                "t_close": last_ohv["close"],
            }
        )

    reporter.print_simul_report(
        entry_date,
        actual_days,
        candidates,
        rows_ok,
        rows_gap,
        rows_miss,
    )

    return rows_ok, rows_gap, rows_miss


if __name__ == "__main__":
    from wye.blsh.domestic import scanner

    simulate(scanner.find_candidates("20260317"))
