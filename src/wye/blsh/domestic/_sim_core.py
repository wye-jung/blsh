"""
SL/TP 시뮬레이션 코어 (일봉 기준, 보수적 처리)

simulator.py 와 grid_search._simulate_one 의 공용 루프.
모듈 레벨 함수만 정의 — 멀티프로세싱에서 pickle 가능.

보수적 처리 원칙:
  - 트레일링 SL: 전일 high 기준으로만 갱신 (당일 high 사용 시 낙관적 편향)
  - TP1 체결 후 같은 봉에서 본전 SL 도달 → 잔량 즉시 본전 청산 (가장 불리한 시나리오)
"""
from __future__ import annotations

from typing import Callable

from wye.blsh.domestic import Tick

SELL_COST_RATE = 0.002  # 증권거래세 + 수수료 합산 (약 0.2%)


def sim_one(
    buy: float,
    sl: float,
    tp1: float,
    tp2: float,
    tp1_ratio: float,
    atr_sl_mult: float,
    atr: float,
    dates: list[str],
    get_ohv: Callable[[str], dict | None],
    sell_cost_rate: float = SELL_COST_RATE,
) -> tuple[str, float, float | None, str | None, dict | None]:
    """
    단일 종목 SL/TP 루프.

    Args:
        buy: 매수가
        sl, tp1, tp2: 초기 SL / 1차 익절가 / 2차 익절가 (호가 단위 반올림은 caller 책임)
        tp1_ratio: TP1 분할매도 비율 (0 < x ≤ 1)
        atr_sl_mult: 트레일링 SL ATR 배수
        atr: ATR 값
        dates: 보유 기간 날짜 목록 (entry_date 포함, 오름차순)
        get_ohv: (date) -> {"open","high","low","close"} | None
        sell_cost_rate: 매도 비용률

    Returns:
        (result_type, ret_pct, exit_price, exit_date, last_ohv)
        result_type: "손절" | "익절(T1+본전손절)" | "익절(전량)" | "익절" | "미확정"
        ret_pct: (pnl / buy) × 100
        exit_price: 청산가 (데이터 부족 시 None)
        exit_date: 청산일 (데이터 부족 시 None)
        last_ohv: 마지막으로 처리한 봉의 OHLCV (데이터 부족 시 None)
    """
    remaining = 1.0
    pnl = 0.0
    t1_done = False
    result_type: str | None = None
    exit_price: float | None = None
    exit_date: str | None = None
    prev_high: float | None = None
    last_ohv: dict | None = None

    for d in dates:
        ohv = get_ohv(d)
        if ohv is None:
            continue
        last_ohv = ohv

        if prev_high is not None:
            # 트레일링 SL: 전일 high 기준 갱신
            trail_sl = Tick.floor_tick(prev_high - atr_sl_mult * atr)
            if trail_sl > sl and trail_sl < prev_high:
                sl = trail_sl

        # 손절
        if ohv["low"] <= sl:
            pnl += (sl - buy) * remaining - sl * remaining * sell_cost_rate
            result_type = "손절"
            exit_price = sl
            exit_date = d
            remaining = 0
            break

        # TP1 분할매도
        if not t1_done and ohv["high"] >= tp1:
            sell_r = min(tp1_ratio, remaining)
            pnl += (tp1 - buy) * sell_r - tp1 * sell_r * sell_cost_rate
            remaining = max(0.0, remaining - sell_r)
            t1_done = True
            if buy > sl:
                sl = buy
            # [보수적] 같은 봉에서 본전 SL도 도달 → 잔량 즉시 본전 청산
            if remaining > 0 and ohv["low"] <= buy:
                pnl += -buy * remaining * sell_cost_rate
                result_type = "익절(T1+본전손절)"
                exit_price = buy
                exit_date = d
                remaining = 0
                break

        # 전일 high 갱신 (TP1 체크 후, TP2 체크 전)
        prev_high = ohv["high"] if prev_high is None else max(prev_high, ohv["high"])

        # TP2 잔량 전량 청산
        if ohv["high"] >= tp2 and remaining > 0:
            pnl += (tp2 - buy) * remaining - tp2 * remaining * sell_cost_rate
            result_type = "익절" if t1_done else "익절(전량)"
            exit_price = tp2
            exit_date = d
            remaining = 0
            break

    # 미확정 → 마지막 봉 종가 청산
    if result_type is None:
        close = last_ohv["close"] if last_ohv else buy
        pnl += (close - buy) * remaining - close * remaining * sell_cost_rate
        result_type = "미확정"
        exit_price = close
        exit_date = dates[-1] if dates else None

    ret_pct = (pnl / buy) * 100 if buy else 0
    return result_type, ret_pct, exit_price, exit_date, last_ohv
