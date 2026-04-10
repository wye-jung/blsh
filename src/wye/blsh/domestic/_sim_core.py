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

import numba as nb
import numpy as np

from wye.blsh.domestic import Tick
from wye.blsh.domestic.config import SELL_COST_RATE

# ─────────────────────────────────────────
# numba JIT 버전 (grid_search 전용)
# ─────────────────────────────────────────
RES_SL = 0        # 손절
RES_TP1_SL = 1    # 익절(T1+본전손절)
RES_TP_FULL = 2   # 익절(전량)
RES_TP = 3        # 익절
RES_HOLD = 4      # 미확정

RESULT_LABELS = ("손절", "익절(T1+본전손절)", "익절(전량)", "익절", "미확정")


@nb.jit(nb.int64(nb.float64), nopython=True, cache=True)
def floor_tick_nb(price):
    """호가 단위 내림 (numba)."""
    if price <= 0:
        return 0
    if price < 1_000:
        tick = 1
    elif price < 5_000:
        tick = 5
    elif price < 10_000:
        tick = 10
    elif price < 50_000:
        tick = 50
    elif price < 100_000:
        tick = 100
    elif price < 500_000:
        tick = 500
    else:
        tick = 1_000
    return int(price) // tick * tick


@nb.jit(nb.int64(nb.float64), nopython=True, cache=True)
def ceil_tick_nb(price):
    """호가 단위 올림 (numba)."""
    if price <= 0:
        return 0
    if price < 1_000:
        tick = 1
    elif price < 5_000:
        tick = 5
    elif price < 10_000:
        tick = 10
    elif price < 50_000:
        tick = 50
    elif price < 100_000:
        tick = 100
    elif price < 500_000:
        tick = 500
    else:
        tick = 1_000
    floored = int(price) // tick * tick
    result = floored if floored >= price else floored + tick
    # 올림이 다음 호가 구간으로 넘어간 경우 재보정
    if result < 1_000:
        ft = 1
    elif result < 5_000:
        ft = 5
    elif result < 10_000:
        ft = 10
    elif result < 50_000:
        ft = 50
    elif result < 100_000:
        ft = 100
    elif result < 500_000:
        ft = 500
    else:
        ft = 1_000
    if result % ft != 0:
        result = (result // ft + 1) * ft
    return result


@nb.jit(nopython=True, cache=True, fastmath=True)
def sim_one_nb(
    buy, sl, tp1, tp2, tp1_ratio, atr_sl_mult, atr,
    opens, highs, lows, closes, n_bars, sell_cost_rate,
):
    """numba JIT SL/TP 루프. sim_one()과 동일 로직, numpy 배열 입력.

    Returns: (result_type_id, ret_pct, exit_price, exit_idx)
    """
    remaining = 1.0
    pnl = 0.0
    t1_done = False
    result_id = RES_HOLD
    exit_price = buy
    exit_idx = n_bars - 1
    prev_high = -1.0

    for i in range(n_bars):
        o = opens[i]
        h = highs[i]
        lo = lows[i]
        c = closes[i]
        if o <= 0.0:
            continue

        # 트레일링 SL: 전일 high 기준
        if prev_high > 0.0:
            trail_sl = floor_tick_nb(prev_high - atr_sl_mult * atr)
            if trail_sl > sl and trail_sl < prev_high:
                sl = float(trail_sl)

        # 손절
        if lo <= sl:
            fill = min(sl, o)
            pnl += (fill - buy) * remaining - fill * remaining * sell_cost_rate
            result_id = RES_SL
            exit_price = fill
            exit_idx = i
            remaining = 0.0
            break

        # TP1 분할매도
        if not t1_done and h >= tp1:
            sell_r = min(tp1_ratio, remaining)
            pnl += (tp1 - buy) * sell_r - tp1 * sell_r * sell_cost_rate
            remaining = max(0.0, remaining - sell_r)
            t1_done = True
            if buy > sl:
                sl = buy
            if remaining == 0.0:
                result_id = RES_TP_FULL
                exit_price = tp1
                exit_idx = i
                break
            # 같은 봉에서 본전 SL 도달
            if lo <= buy:
                pnl += -buy * remaining * sell_cost_rate
                result_id = RES_TP1_SL
                exit_price = buy
                exit_idx = i
                remaining = 0.0
                break

        # 전일 high 갱신
        prev_high = h if prev_high < 0.0 else max(prev_high, h)

        # TP2 전량 청산
        if h >= tp2 and remaining > 0.0:
            pnl += (tp2 - buy) * remaining - tp2 * remaining * sell_cost_rate
            result_id = RES_TP if t1_done else RES_TP_FULL
            exit_price = tp2
            exit_idx = i
            remaining = 0.0
            break

    # 미확정: 마지막 봉 종가
    if result_id == RES_HOLD:
        last_c = closes[exit_idx] if exit_idx < n_bars and closes[exit_idx] > 0 else buy
        pnl += (last_c - buy) * remaining - last_c * remaining * sell_cost_rate
        exit_price = last_c

    ret_pct = (pnl / buy) * 100.0 if buy > 0.0 else 0.0
    return result_id, ret_pct, exit_price, exit_idx


@nb.jit(nopython=True, cache=True, fastmath=True)
def backtest_nb(
    buy_arr, atr_arr, score_arr, mode_id_arr, n_bars_arr,
    opens_2d, highs_2d, lows_2d, closes_2d,
    min_score, sl_mult, tp1_mult, tp2_mult, tp1_ratio,
    max_hold_rev, max_hold_mix, max_hold_mom,
    sell_cost_rate,
):
    """전체 백테스트 루프 (numba JIT).

    flat numpy 배열로 전환된 신호 데이터를 받아 Python 오버헤드 없이 실행.
    Returns: (trades, wins, losses, holds, total_ret, ret_sq)
    """
    n_sigs = len(buy_arr)
    trades = 0
    wins = 0
    losses = 0
    holds = 0
    total_ret = 0.0
    ret_sq = 0.0

    for i in range(n_sigs):
        eff_score = int(score_arr[i])

        if eff_score < min_score:
            continue

        buy = buy_arr[i]
        atr = atr_arr[i]
        sl = float(floor_tick_nb(buy - sl_mult * atr))
        tp1 = float(ceil_tick_nb(buy + tp1_mult * atr))
        tp2 = float(ceil_tick_nb(buy + tp2_mult * atr))

        mid = mode_id_arr[i]
        if mid == 2:
            max_d = max_hold_mom
        elif mid == 1:
            max_d = max_hold_mix
        else:
            max_d = max_hold_rev

        nb_i = n_bars_arr[i]
        n = max_d + 1 if max_d + 1 < nb_i else nb_i

        res_id, ret_pct, _, _ = sim_one_nb(
            buy, sl, tp1, tp2, tp1_ratio, sl_mult, atr,
            opens_2d[i], highs_2d[i], lows_2d[i], closes_2d[i],
            n, sell_cost_rate,
        )

        trades += 1
        total_ret += ret_pct
        ret_sq += ret_pct * ret_pct
        if res_id <= 3:
            if res_id == 0:
                losses += 1
            else:
                wins += 1
        else:
            holds += 1

    return trades, wins, losses, holds, total_ret, ret_sq


@nb.jit(nopython=True, cache=True, fastmath=True)
def backtest_scores_nb(
    flag_mask_arr, supply_bonus_arr, has_pov_arr,
    buy_arr, atr_arr, n_bars_arr,
    opens_2d, highs_2d, lows_2d, closes_2d,
    score_values, mom_mask, rev_mask, n_flags,
    min_score,
    sl_mult, tp1_mult, tp2_mult, tp1_ratio,
    max_hold_rev, max_hold_mix, max_hold_mom,
    sell_cost_rate,
):
    """backtest_scores의 numba 버전. 플래그 비트마스크 + 점수 배열로 score 재계산.

    Args:
        flag_mask_arr: (N,) int64 — 신호별 플래그 비트마스크
        supply_bonus_arr: (N,) int64 — 캡 적용 수급 보너스
        has_pov_arr: (N,) int64 — P_OV 여부 (0/1)
        score_values: (n_flags,) int64 — 플래그 인덱스별 점수
        mom_mask, rev_mask: int64 — 모멘텀/전환 플래그 비트마스크
    Returns: (trades, wins, losses, holds, total_ret, ret_sq)
    """
    n_sigs = len(buy_arr)
    trades = 0
    wins = 0
    losses = 0
    holds = 0
    total_ret = 0.0
    ret_sq = 0.0

    for i in range(n_sigs):
        fm = flag_mask_arr[i]
        mom_cnt = 0
        rev_cnt = 0
        mom_score = 0
        rev_score = 0
        neu_score = 0

        for b in range(n_flags):
            bit = nb.int64(1) << nb.int64(b)
            if fm & bit:
                sv = score_values[b]
                if bit & mom_mask:
                    mom_cnt += 1
                    mom_score += sv
                elif bit & rev_mask:
                    rev_cnt += 1
                    rev_score += sv
                else:
                    neu_score += sv

        # mode 분류 + tech_score
        if mom_cnt >= 2 and mom_cnt > rev_cnt:
            mode_id = 2  # MOM
            tech_score = mom_score + neu_score
        elif rev_cnt >= 2 and rev_cnt > mom_cnt:
            mode_id = 0  # REV
            tech_score = rev_score + neu_score
        elif mom_cnt > 0 and rev_cnt > 0:
            mode_id = 1  # MIX
            tech_score = mom_score if mom_score > rev_score else rev_score
            tech_score += neu_score
        else:
            continue  # WEAK

        eff_score = tech_score + supply_bonus_arr[i]
        if has_pov_arr[i]:
            eff_score -= 1

        if eff_score < min_score:
            continue

        buy = buy_arr[i]
        atr = atr_arr[i]
        sl = float(floor_tick_nb(buy - sl_mult * atr))
        tp1 = float(ceil_tick_nb(buy + tp1_mult * atr))
        tp2 = float(ceil_tick_nb(buy + tp2_mult * atr))

        if mode_id == 2:
            max_d = max_hold_mom
        elif mode_id == 1:
            max_d = max_hold_mix
        else:
            max_d = max_hold_rev

        nb_i = n_bars_arr[i]
        n = max_d + 1 if max_d + 1 < nb_i else nb_i

        res_id, ret_pct, _, _ = sim_one_nb(
            buy, sl, tp1, tp2, tp1_ratio, sl_mult, atr,
            opens_2d[i], highs_2d[i], lows_2d[i], closes_2d[i],
            n, sell_cost_rate,
        )

        trades += 1
        total_ret += ret_pct
        ret_sq += ret_pct * ret_pct
        if res_id == 0:
            losses += 1
        elif res_id <= 3:
            wins += 1
        else:
            holds += 1

    return trades, wins, losses, holds, total_ret, ret_sq


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
            fill = min(sl, ohv["open"])  # 갭 하락 시 시가에 체결
            pnl += (fill - buy) * remaining - fill * remaining * sell_cost_rate
            result_type = "손절"
            exit_price = fill
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
            # 전량 익절(tp1_ratio=1.0) → 즉시 종료
            if remaining == 0:
                result_type = "익절(전량)"
                exit_price = tp1
                exit_date = d
                break
            # [보수적] 같은 봉에서 본전 SL도 도달 → 잔량 즉시 본전 청산
            if ohv["low"] <= buy:
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
