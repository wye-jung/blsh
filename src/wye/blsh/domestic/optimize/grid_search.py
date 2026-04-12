"""
Grid Search 최적화
──────────────────────────────────
최적 파라미터 탐색

실행:
    uv run python -m wye.blsh.domestic.optimize.grid_search
    uv run python -m wye.blsh.domestic.optimize.grid_search --years 2
    uv run python -m wye.blsh.domestic.optimize.grid_search --rebuild
"""

import argparse
import logging
import multiprocessing as mp
import os
import time
from dataclasses import dataclass
from datetime import datetime
from itertools import product
from pathlib import Path

from wye.blsh.common import dtutils
from wye.blsh.domestic import Tick
from wye.blsh.domestic._sim_core import (
    sim_one,
    sim_one_nb, floor_tick_nb, ceil_tick_nb,
    backtest_nb, backtest_scores_nb,
    RESULT_LABELS, RES_HOLD,
)
from wye.blsh.domestic.config import SELL_COST_RATE, SIGNAL_SCORES_MOM, SIGNAL_SCORES_REV, SUPPLY_CAP
from wye.blsh.domestic.optimize._cache import (
    build_or_load, OptCache, CACHE_DIR,
    _REVERSAL_FLAGS, _MOMENTUM_FLAGS, _ALL_FLAGS, _classify_mode,
    FLAG_ORDER, N_FLAGS, MOM_MASK, REV_MASK,
)

log = logging.getLogger(__name__)

# fork 방식으로 캐시를 자식 프로세스에 공유 (복사 없이 CoW)
_WORKER_CACHE: OptCache | None = None


def _backtest_worker(args: tuple) -> tuple["Params", "Stats"]:
    keys, combo = args
    p = Params(**dict(zip(keys, combo)))
    return p, backtest(_WORKER_CACHE, p)


# ─────────────────────────────────────────
# 파라미터 + 결과
# ─────────────────────────────────────────
@dataclass(frozen=True)
class Params:
    invest_min_score: int
    atr_sl_mult: float
    atr_tp_mult: float
    max_hold_days_rev: int
    max_hold_days_mix: int
    max_hold_days_mom: int
    tp1_mult: float  # 1차 익절 ATR 배수 (e.g. 0.7, 1.0, 1.5)
    tp1_ratio: float  # 1차 익절 매도 비율 (e.g. 0.3, 0.5, 0.7)
    max_idx_drop: float = 1.0  # 지수 MA20 괴리율 하한 (1.0 = 비활성)
    atr_cap: float = 0.50  # ATR 상한 (매수가 대비 비율)

    def label(self) -> str:
        idx_lbl = "idx=off" if self.max_idx_drop >= 1.0 else f"idx≤{self.max_idx_drop:.0%}"
        atr_lbl = "" if self.atr_cap >= 0.50 else f" atrCap={self.atr_cap:.0%}"
        return (
            f"score≥{self.invest_min_score} "
            f"SL={self.atr_sl_mult:.1f} TP1={self.tp1_mult:.1f}({self.tp1_ratio:.0%}) "
            f"TP2={self.atr_tp_mult:.1f} "
            f"REV={self.max_hold_days_rev}d MIX={self.max_hold_days_mix}d "
            f"MOM={self.max_hold_days_mom}d "
            f"{idx_lbl}{atr_lbl}".rstrip()
        )


@dataclass
class Stats:
    trades: int = 0
    wins: int = 0
    losses: int = 0
    holds: int = 0
    total_ret: float = 0.0
    ret_sq: float = 0.0
    w_total_ret: float = 0.0
    w_ret_sq: float = 0.0
    w_sum: float = 0.0

    @property
    def win_rate(self) -> float:
        return self.wins / self.trades * 100 if self.trades else 0

    @property
    def avg_ret(self) -> float:
        return self.total_ret / self.trades if self.trades else 0

    @property
    def ret_std(self) -> float:
        """수익률 표준편차."""
        if self.trades < 2:
            return 0.0
        import math
        var = self.ret_sq / self.trades - self.avg_ret ** 2
        return math.sqrt(max(var, 0.0))

    @property
    def metric(self) -> float:
        """최적화 지표: 시간 가중 Sharpe-like 지표.

        최근 거래에 더 높은 가중치를 부여한 (w_avg / w_std) × sqrt(min(trades, MAX_TRADES)).
        거래 수 보상은 MAX_TRADES에서 포화되어 거래 수 인플레이션을 방지.
        30건 미만은 통계 무의미로 제외.
        """
        if self.trades < 30:
            return -9999
        import math
        n = min(self.trades, MAX_TRADES)
        if self.w_sum > 0:
            w_avg = self.w_total_ret / self.w_sum
            w_var = self.w_ret_sq / self.w_sum - w_avg ** 2
            w_std = math.sqrt(max(w_var, 0.0))
        else:
            w_avg = self.avg_ret
            w_std = self.ret_std
        if w_std <= 0:
            return w_avg * math.sqrt(n)
        return (w_avg / w_std) * math.sqrt(n)


# ─────────────────────────────────────────
# 시뮬레이션 (1건)
# ─────────────────────────────────────────
def _simulate_one(
    sig: dict,
    entry_date: str,
    ohlcv_idx: dict,
    params: Params,
    hold_dates: list[str],
) -> tuple[str, float] | None:
    """1개 후보 시뮬레이션 (numba JIT). (result_type, ret_pct) 반환, 스킵이면 None."""
    ticker = sig["ticker"]
    atr = sig["atr"]
    cache: OptCache = _WORKER_CACHE

    # numba 경로: numpy 배열 사용
    if cache.ohlcv_arrays is not None:
        entry_idx = cache.date_to_idx.get(entry_date)
        if entry_idx is None:
            return None
        arr = cache.ohlcv_arrays.get(ticker)
        if arr is None or arr[entry_idx, 0] <= 0:
            return None
        if arr[entry_idx, 0] > sig["entry_price"]:
            return None

        buy = arr[entry_idx, 0]
        effective_atr = min(atr, buy * params.atr_cap)
        sl = float(floor_tick_nb(buy - params.atr_sl_mult * effective_atr))
        tp1 = float(ceil_tick_nb(buy + params.tp1_mult * effective_atr))
        tp2 = float(ceil_tick_nb(buy + params.atr_tp_mult * effective_atr))

        mode = sig["mode"]
        if mode == "MOM":
            max_d = params.max_hold_days_mom
        elif mode == "MIX":
            max_d = params.max_hold_days_mix
        else:
            max_d = params.max_hold_days_rev

        # hold_dates → 인덱스 배열 (거래정지 등으로 비연속일 수 있음)
        import numpy as _np
        d2i = cache.date_to_idx
        indices = [d2i[d] for d in hold_dates if d >= entry_date and d in d2i]
        indices = indices[: max_d + 1]
        if not indices:
            return None

        idx_arr = _np.array(indices)
        res_id, ret, _, _ = sim_one_nb(
            buy, sl, tp1, tp2, params.tp1_ratio,
            params.atr_sl_mult, atr,
            arr[idx_arr, 0], arr[idx_arr, 1],
            arr[idx_arr, 2], arr[idx_arr, 3],
            len(idx_arr), SELL_COST_RATE,
            params.atr_cap,
        )

        label = RESULT_LABELS[res_id]
        if label == "미확정" and max_d == 0:
            label = "데이청산"
        return label, ret

    # fallback: 기존 dict 경로
    t1 = ohlcv_idx.get((ticker, entry_date))
    if t1 is None:
        return None
    if t1["open"] > sig["entry_price"]:
        return None

    buy = t1["open"]
    effective_atr = min(atr, buy * params.atr_cap)
    sl = Tick.floor_tick(buy - params.atr_sl_mult * effective_atr)
    tp1 = Tick.ceil_tick(buy + params.tp1_mult * effective_atr)
    tp2 = Tick.ceil_tick(buy + params.atr_tp_mult * effective_atr)

    mode = sig["mode"]
    if mode == "MOM":
        max_d = params.max_hold_days_mom
    elif mode == "MIX":
        max_d = params.max_hold_days_mix
    else:
        max_d = params.max_hold_days_rev

    dates = [d for d in hold_dates if d >= entry_date][: max_d + 1]
    if not dates:
        return None

    result_type, ret, _, _, _ = sim_one(
        buy=buy, sl=sl, tp1=tp1, tp2=tp2,
        tp1_ratio=params.tp1_ratio,
        atr_sl_mult=params.atr_sl_mult,
        atr=atr, dates=dates,
        get_ohv=lambda d: ohlcv_idx.get((ticker, d)),
        atr_cap=params.atr_cap,
    )

    if result_type == "미확정" and max_d == 0:
        result_type = "데이청산"
    return result_type, ret


# ─────────────────────────────────────────
# 전체 기간 백테스트
# ─────────────────────────────────────────
def backtest(cache: OptCache, params: Params) -> Stats:
    """캐시 데이터로 전체 기간 백테스트.

    flat numpy 배열이 있으면 numba JIT 경로 사용, 없으면 dict 경로 fallback.
    """
    # ── numba 경로: flat 배열 사용
    if hasattr(cache, "flat_buy") and cache.flat_buy is not None:
        _max_di = int(max(cache.flat_date_idx)) if len(cache.flat_date_idx) > 0 else 0
        trades, wins, losses, holds, total_ret, ret_sq, w_total_ret, w_ret_sq, w_sum = backtest_nb(
            cache.flat_buy, cache.flat_atr, cache.flat_score,
            cache.flat_mode_id, cache.flat_n_bars,
            cache.flat_opens, cache.flat_highs, cache.flat_lows, cache.flat_closes,
            params.invest_min_score,
            params.atr_sl_mult, params.tp1_mult, params.atr_tp_mult,
            params.tp1_ratio,
            params.max_hold_days_rev, params.max_hold_days_mix, params.max_hold_days_mom,
            SELL_COST_RATE,
            cache.flat_idx_gap, params.max_idx_drop,
            params.atr_cap,
            cache.flat_date_idx, _max_di, HALF_LIFE,
        )
        st = Stats()
        st.trades = trades
        st.wins = wins
        st.losses = losses
        st.holds = holds
        st.total_ret = total_ret
        st.ret_sq = ret_sq
        st.w_total_ret = w_total_ret
        st.w_ret_sq = w_ret_sq
        st.w_sum = w_sum
        return st

    # ── fallback: dict 경로
    st = Stats()
    _max_hold = (params.max_hold_days_rev, params.max_hold_days_mix, params.max_hold_days_mom)
    _min_score = params.invest_min_score
    _sl_mult = params.atr_sl_mult
    _tp1_mult = params.tp1_mult
    _tp2_mult = params.atr_tp_mult
    _tp1_ratio = params.tp1_ratio
    _cost = SELL_COST_RATE
    _max_idx_drop = params.max_idx_drop
    _atr_cap = params.atr_cap

    for base_date in cache.scan_dates:
        sigs = cache.signals.get(base_date)
        if not sigs:
            continue

        for sig in sigs:
            if sig.get("_skip", True):
                continue

            effective_score = sig["score"]

            if effective_score < _min_score:
                continue

            if sig.get("idx_gap", 0.0) < -_max_idx_drop:
                continue

            buy = sig["_buy"]
            atr = sig["atr"]
            effective_atr = min(atr, buy * _atr_cap)
            sl = float(floor_tick_nb(buy - _sl_mult * effective_atr))
            tp1 = float(ceil_tick_nb(buy + _tp1_mult * effective_atr))
            tp2 = float(ceil_tick_nb(buy + _tp2_mult * effective_atr))

            max_d = _max_hold[sig["_mode_id"]]
            n = min(max_d + 1, sig["_n_bars"])

            res_id, ret_pct, _, _ = sim_one_nb(
                buy, sl, tp1, tp2, _tp1_ratio, _sl_mult, atr,
                sig["_opens"], sig["_highs"], sig["_lows"], sig["_closes"],
                n, _cost,
                _atr_cap,
            )

            st.trades += 1
            st.total_ret += ret_pct
            st.ret_sq += ret_pct * ret_pct
            if res_id <= 3:
                if res_id == 0:
                    st.losses += 1
                else:
                    st.wins += 1
            else:
                st.holds += 1

    return st


# ─────────────────────────────────────────
# 개별 거래 기록 백테스트 (진단용)
# ─────────────────────────────────────────
@dataclass
class TradeRecord:
    ticker: str
    base_date: str
    mode: str       # MOM/REV/MIX
    score: int
    flags: str      # comma-separated
    market: str     # KOSPI/KOSDAQ
    result: str     # SL/TP_FULL/TP1/TP1_SL/HOLD
    ret_pct: float
    entry_price: float


# 결과 ID → TradeRecord용 라벨 매핑
_RESULT_ID_TO_LABEL = {
    0: "SL",
    1: "TP1_SL",
    2: "TP_FULL",
    3: "TP1+TP2",
    4: "HOLD",  # RES_HOLD (미확정)
}


def backtest_with_trades(
    cache: OptCache,
    params: Params,
    start_date: str | None = None,
    end_date: str | None = None,
) -> tuple[Stats, list[TradeRecord]]:
    """캐시 데이터로 백테스트하며 개별 거래 기록을 수집 (진단용).

    dict 기반 루프를 사용하여 각 거래의 메타데이터를 TradeRecord로 기록.
    start_date/end_date 지정 시 해당 기간만 필터링.
    """
    target = cache
    if start_date or end_date:
        target = cache.slice_by_dates(
            start_date or cache.scan_dates[0],
            end_date or cache.scan_dates[-1],
        )
        target.flatten_signals()

    st = Stats()
    trades: list[TradeRecord] = []
    _max_hold = (params.max_hold_days_rev, params.max_hold_days_mix, params.max_hold_days_mom)
    _min_score = params.invest_min_score
    _sl_mult = params.atr_sl_mult
    _tp1_mult = params.tp1_mult
    _tp2_mult = params.atr_tp_mult
    _tp1_ratio = params.tp1_ratio
    _cost = SELL_COST_RATE
    _max_idx_drop = params.max_idx_drop
    _atr_cap = params.atr_cap
    _MODE_LABELS = {0: "REV", 1: "MIX", 2: "MOM"}

    for base_date in target.scan_dates:
        sigs = target.signals.get(base_date)
        if not sigs:
            continue

        for sig in sigs:
            if sig.get("_skip", True):
                continue

            effective_score = sig["score"]

            if effective_score < _min_score:
                continue

            if sig.get("idx_gap", 0.0) < -_max_idx_drop:
                continue

            buy = sig["_buy"]
            atr = sig["atr"]
            effective_atr = min(atr, buy * _atr_cap)
            sl = float(floor_tick_nb(buy - _sl_mult * effective_atr))
            tp1 = float(ceil_tick_nb(buy + _tp1_mult * effective_atr))
            tp2 = float(ceil_tick_nb(buy + _tp2_mult * effective_atr))

            max_d = _max_hold[sig["_mode_id"]]
            n = min(max_d + 1, sig["_n_bars"])

            res_id, ret_pct, _, _ = sim_one_nb(
                buy, sl, tp1, tp2, _tp1_ratio, _sl_mult, atr,
                sig["_opens"], sig["_highs"], sig["_lows"], sig["_closes"],
                n, _cost,
                _atr_cap,
            )

            st.trades += 1
            st.total_ret += ret_pct
            st.ret_sq += ret_pct * ret_pct
            if res_id <= 3:
                if res_id == 0:
                    st.losses += 1
                else:
                    st.wins += 1
            else:
                st.holds += 1

            result_label = _RESULT_ID_TO_LABEL.get(res_id, "HOLD")
            trades.append(TradeRecord(
                ticker=sig["ticker"],
                base_date=base_date,
                mode=_MODE_LABELS.get(sig["_mode_id"], "REV"),
                score=effective_score,
                flags=sig.get("flags", ""),
                market=sig.get("market", ""),
                result=result_label,
                ret_pct=ret_pct,
                entry_price=buy,
            ))

    return st, trades


# ─────────────────────────────────────────
# 1단계: 신호 점수 최적화 (승률 기준)
# ─────────────────────────────────────────
SCORE_GRID_MOM_A = {
    # MOM 고배점 (4개): -1~3
    "MGC": [-1, 0, 1, 2, 3],
    "W52": [-1, 0, 1, 2, 3],
    "PB":  [-1, 0, 1, 2, 3],
    "LB":  [-1, 0, 1, 2, 3],
}  # 5^4 = 625 콤보

SCORE_GRID_MOM_B = {
    # MOM 저배점 (3개) + NEU (3개, MOM 맥락)
    "VS":   [-1, 0, 1, 2],  # 현재 0 → -1 허용
    "MAA":  [0, 1, 2],
    "OBV":  [0, 1, 2],
    "MPGC": [0, 1, 2],
    "BBM":  [0, 1, 2],
    "SGC":  [0, 1, 2],
}  # 4 x 3^5 = 972 콤보

SCORE_GRID_REV = {
    # REV 전체 (6개): -1~3
    "MS":  [-1, 0, 1, 2, 3],
    "RBO": [-1, 0, 1, 2, 3],
    "ROV": [-1, 0, 1, 2, 3],
    "BBL": [-1, 0, 1, 2, 3],
    "HMR": [-1, 0, 1, 2, 3],
    "BE":  [-1, 0, 1, 2, 3],
}  # 5^6 = 15,625 콤보

SCORE_GRID_REV_NEU = {
    # NEU (3개, REV 맥락)
    "MPGC": [0, 1, 2],
    "BBM":  [0, 1, 2],
    "SGC":  [0, 1, 2],
}  # 3^3 = 27 콤보


def _calc_score_with(flags: set, mode: str, scores_mom: dict, scores_rev: dict) -> int:
    """모드별 scores dict로 tech_score 재계산."""
    mom = sum(scores_mom.get(f, 0) for f in flags & _MOMENTUM_FLAGS)
    rev = sum(scores_rev.get(f, 0) for f in flags & _REVERSAL_FLAGS)
    neu_mom = sum(scores_mom.get(f, 0) for f in flags - _ALL_FLAGS)
    neu_rev = sum(scores_rev.get(f, 0) for f in flags - _ALL_FLAGS)
    if mode == "MOM":
        return mom + neu_mom
    if mode == "REV":
        return rev + neu_rev
    if mode == "MIX":
        return max(mom + neu_mom, rev + neu_rev)
    return mom + rev + max(neu_mom, neu_rev)


def backtest_scores(
    cache: OptCache, params: Params, scores_mom: dict, scores_rev: dict,
    min_score: int, mode_filter: int = 7,
) -> Stats:
    """모드별 SIGNAL_SCORES_MOM/REV로 백테스트 (사전 계산된 OHLCV 사용)."""
    st = Stats()
    _MODE_MAP = {"MOM": 2, "MIX": 1, "REV": 0}
    _MODE_BIT = {"MOM": 4, "MIX": 2, "REV": 1}
    _max_hold = (params.max_hold_days_rev, params.max_hold_days_mix, params.max_hold_days_mom)
    _sl_mult = params.atr_sl_mult
    _tp1_mult = params.tp1_mult
    _tp2_mult = params.atr_tp_mult
    _tp1_ratio = params.tp1_ratio
    _cost = SELL_COST_RATE
    _max_idx_drop = params.max_idx_drop
    _atr_cap = params.atr_cap

    for base_date in cache.scan_dates:
        sigs = cache.signals.get(base_date)
        if not sigs:
            continue

        for sig in sigs:
            if sig.get("_skip", True):
                continue

            if sig.get("idx_gap", 0.0) < -_max_idx_drop:
                continue

            # 신호 점수 재계산
            flag_str = sig.get("flags", "")
            flags = set(flag_str.split(",")) if flag_str else set()
            flags.discard("")
            mode = _classify_mode(flags)
            if mode not in _MODE_MAP:
                continue
            if not (mode_filter & _MODE_BIT[mode]):
                continue

            tech_score = _calc_score_with(flags, mode, scores_mom, scores_rev)
            supply_bonus = min(sig.get("raw_supply_bonus", 0), SUPPLY_CAP)
            effective_score = tech_score + supply_bonus
            if "P_OV" in flags:
                effective_score -= 1

            if effective_score < min_score:
                continue

            buy = sig["_buy"]
            atr = sig["atr"]
            effective_atr = min(atr, buy * _atr_cap)
            sl = float(floor_tick_nb(buy - _sl_mult * effective_atr))
            tp1 = float(ceil_tick_nb(buy + _tp1_mult * effective_atr))
            tp2 = float(ceil_tick_nb(buy + _tp2_mult * effective_atr))

            max_d = _max_hold[_MODE_MAP[mode]]
            n = min(max_d + 1, sig["_n_bars"])

            res_id, ret_pct, _, _ = sim_one_nb(
                buy, sl, tp1, tp2, _tp1_ratio, _sl_mult, atr,
                sig["_opens"], sig["_highs"], sig["_lows"], sig["_closes"],
                n, _cost,
                _atr_cap,
            )

            st.trades += 1
            st.total_ret += ret_pct
            st.ret_sq += ret_pct * ret_pct
            if res_id == 0:
                st.losses += 1
            elif res_id <= 3:
                st.wins += 1
            else:
                st.holds += 1

    return st


def _score_worker(args):
    """1단계 워커: (score_keys, score_combo, params_dict, base_mom, base_rev, target, mode_filter)."""
    score_keys, score_combo, params_dict, base_mom, base_rev, target, mode_filter = args
    scores_mom = dict(base_mom)
    scores_rev = dict(base_rev)
    combo_dict = dict(zip(score_keys, score_combo))

    if target == "mom":
        scores_mom.update(combo_dict)
    else:
        scores_rev.update(combo_dict)

    params = Params(**params_dict)
    cache = _WORKER_CACHE
    if hasattr(cache, "flat_flag_mask") and cache.flat_flag_mask is not None:
        import numpy as _np
        sv_mom = _np.array([scores_mom.get(f, 0) for f in FLAG_ORDER], dtype=_np.int64)
        sv_rev = _np.array([scores_rev.get(f, 0) for f in FLAG_ORDER], dtype=_np.int64)
        _max_di = int(max(cache.flat_date_idx)) if len(cache.flat_date_idx) > 0 else 0
        trades, wins, losses, holds, total_ret, ret_sq, w_total_ret, w_ret_sq, w_sum = backtest_scores_nb(
            cache.flat_flag_mask, cache.flat_supply_bonus, cache.flat_has_pov,
            cache.flat_buy, cache.flat_atr, cache.flat_n_bars,
            cache.flat_opens, cache.flat_highs, cache.flat_lows, cache.flat_closes,
            sv_mom, sv_rev, MOM_MASK, REV_MASK, N_FLAGS,
            _WORKER_MIN_SCORE,
            params.atr_sl_mult, params.tp1_mult, params.atr_tp_mult,
            params.tp1_ratio,
            params.max_hold_days_rev, params.max_hold_days_mix, params.max_hold_days_mom,
            SELL_COST_RATE,
            cache.flat_idx_gap, params.max_idx_drop,
            params.atr_cap,
            cache.flat_date_idx, _max_di, HALF_LIFE,
            mode_filter,
        )
        st = Stats()
        st.trades = trades
        st.wins = wins
        st.losses = losses
        st.holds = holds
        st.total_ret = total_ret
        st.ret_sq = ret_sq
        st.w_total_ret = w_total_ret
        st.w_ret_sq = w_ret_sq
        st.w_sum = w_sum
    else:
        st = backtest_scores(cache, params, scores_mom, scores_rev, _WORKER_MIN_SCORE, mode_filter)

    return (scores_mom, scores_rev), st


def optimize_scores(
    cache: OptCache, params: Params, n_workers: int,
    min_score: int | None = None,
    current_scores_mom: dict | None = None,
    current_scores_rev: dict | None = None,
    score_grid: dict | None = None,
    target: str = "mom",
    mode_filter: int = 7,
    label: str = "",
) -> tuple[dict, dict]:
    """모드별 신호 점수 그리드 탐색.

    Args:
        min_score: 진입 최소 점수. None이면 GRID 최소값 사용.
        current_scores_mom: MOM 점수 dict. None이면 SIGNAL_SCORES_MOM 초기값.
        current_scores_rev: REV 점수 dict. None이면 SIGNAL_SCORES_REV 초기값.
        score_grid: 탐색 대상 그리드.
        target: "mom" 또는 "rev" — 탐색 대상 dict.
        mode_filter: 비트마스크 (1=REV, 2=MIX, 4=MOM). 7=전체.
        label: 로그 라벨.
    Returns: (scores_mom, scores_rev) 튜플.
    """
    global _WORKER_MIN_SCORE
    _min = min_score if min_score is not None else _SCORE_MIN_SCORE
    _WORKER_MIN_SCORE = _min

    grid = score_grid if score_grid is not None else SCORE_GRID_MOM_A
    score_keys = list(grid.keys())
    all_score_combos = list(product(*[grid[k] for k in score_keys]))

    params_dict = {
        "invest_min_score": params.invest_min_score,
        "atr_sl_mult": params.atr_sl_mult,
        "atr_tp_mult": params.atr_tp_mult,
        "max_hold_days_rev": params.max_hold_days_rev,
        "max_hold_days_mix": params.max_hold_days_mix,
        "max_hold_days_mom": params.max_hold_days_mom,
        "tp1_mult": params.tp1_mult,
        "tp1_ratio": params.tp1_ratio,
        "max_idx_drop": params.max_idx_drop,
        "atr_cap": params.atr_cap,
    }

    src_mom = current_scores_mom if current_scores_mom is not None else dict(SIGNAL_SCORES_MOM)
    src_rev = current_scores_rev if current_scores_rev is not None else dict(SIGNAL_SCORES_REV)

    # 탐색 대상 외 플래그는 고정
    if target == "mom":
        base_mom = {k: v for k, v in src_mom.items() if k not in grid}
    else:
        base_mom = dict(src_mom)
    if target == "rev":
        base_rev = {k: v for k, v in src_rev.items() if k not in grid}
    else:
        base_rev = dict(src_rev)

    tasks = [
        (score_keys, combo, params_dict, base_mom, base_rev, target, mode_filter)
        for combo in all_score_combos
    ]
    tag = f" {label}" if label else ""
    log.info(f"\n{'─' * 70}")
    log.info(
        f"  [1단계{tag}] 신호 점수 최적화: {len(tasks):,}개 조합"
        f"  (min_score={_min}, target={target}, mode_filter={mode_filter})"
    )
    log.info(f"{'─' * 70}")

    results: list[tuple[tuple[dict, dict], Stats]] = []
    t0 = time.time()
    chunk = max(10, min(200, len(tasks) // (n_workers * 32)))

    with mp.Pool(processes=n_workers) as pool:
        for i, (scores_pair, st) in enumerate(
            pool.imap_unordered(_score_worker, tasks, chunksize=chunk)
        ):
            results.append((scores_pair, st))
            n = i + 1
            if n % 500 == 0 or n == len(tasks):
                elapsed = time.time() - t0
                log.info(f"  {n:>6d}/{len(tasks)}  ({elapsed:.0f}초)")

    results.sort(key=lambda x: x[1].metric, reverse=True)

    elapsed = time.time() - t0
    src = src_mom if target == "mom" else src_rev
    if results and results[0][1].metric > -9999:
        (best_mom, best_rev), best_st = results[0]
        log.info(
            f"\n  ★ 1단계{tag} 최적:"
            f"  {best_st.trades}건  승률 {best_st.win_rate:.1f}%"
            f"  평균 {best_st.avg_ret:+.2f}%  총 {best_st.total_ret:+.1f}%"
            f"  [{elapsed:.0f}초]"
        )
        best = best_mom if target == "mom" else best_rev
        changed = {k: v for k, v in best.items() if v != src.get(k)}
        if changed:
            log.info(f"    변경: {changed}")
        return best_mom, best_rev
    log.warning(f"  1단계{tag}: 유효 결과 없음 → 기존 값 유지")
    return dict(src_mom), dict(src_rev)


def recalc_cache_scores(cache: OptCache, scores_mom: dict, scores_rev: dict):
    """캐시 signal의 score를 SIGNAL_SCORES_MOM/REV로 재계산."""
    for date, sigs in cache.signals.items():
        for sig in sigs:
            flag_str = sig.get("flags", "")
            flags = set(flag_str.split(",")) if flag_str else set()
            flags.discard("")
            mode = _classify_mode(flags)
            sig["mode"] = mode
            tech_score = _calc_score_with(flags, mode, scores_mom, scores_rev)
            sig["tech_score"] = tech_score
            supply_bonus = min(sig.get("raw_supply_bonus", 0), SUPPLY_CAP)
            score = tech_score + supply_bonus
            if "P_OV" in flags:
                score -= 1
            sig["score"] = score


# ─────────────────────────────────────────
# 그리드 정의
# ─────────────────────────────────────────
HALF_LIFE: float = 120  # 시간 가중 반감기 (일 수 기준 인덱스)
MAX_TRADES: int = 2000  # metric의 sqrt(trades) 보상 cap (거래 수 인플레이션 방지)

GRID = {
    "invest_min_score": [9, 10, 11, 12, 13],
    "atr_sl_mult": [1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0],
    "atr_tp_mult": [1.5, 2.0, 2.5, 3.0, 4.0, 5.0],
    "max_hold_days_rev": [3, 5, 7, 10, 15, 20],
    "max_hold_days_mix": [2, 3, 5, 7, 10],
    "max_hold_days_mom": [1, 2, 3],
    "tp1_mult": [0.7, 1.0, 1.5, 2.0, 2.5],
    "tp1_ratio": [0.3, 0.5, 0.7, 1.0],
    "max_idx_drop": [0.03, 0.05, 0.10, 1.0],
    "atr_cap": [0.03, 0.05, 0.08, 0.50],
}

_SCORE_MIN_SCORE = min(GRID["invest_min_score"])  # 1단계 진입 기준: GRID 최소값
_WORKER_MIN_SCORE: int = _SCORE_MIN_SCORE  # 1단계 워커용 min_score (동적 갱신)


# ─────────────────────────────────────────
# 리포트
# ─────────────────────────────────────────
def _report(ranked: list[tuple[Params, Stats]], elapsed: float):
    log.info("")
    log.info("=" * 100)
    log.info(f"  최적화 결과  (Top 15)   [{elapsed:.0f}초]")
    log.info("=" * 100)
    log.info(
        f"  {'#':>3s}  {'거래':>6s}  {'승률':>6s}  {'평균수익':>8s}  {'표준편차':>8s}  {'총수익':>10s}  │ 파라미터"
    )
    log.info("-" * 110)

    for rank, (p, s) in enumerate(ranked[:15], 1):
        log.info(
            f"  {rank:3d}  {s.trades:>5d}건  {s.win_rate:>5.1f}%  "
            f"{s.avg_ret:>+7.2f}%  {s.ret_std:>7.2f}%  {s.total_ret:>+9.1f}%  │ {p.label()}"
        )

    log.info("-" * 100)
    if ranked:
        best_p, best_s = ranked[0]
        log.info(f"\n  ★ 최적 파라미터:")
        log.info(f"    INVEST_MIN_SCORE = {best_p.invest_min_score}")
        log.info(f"    ATR_SL_MULT      = {best_p.atr_sl_mult}")
        log.info(
            f"    TP1_MULT         = {best_p.tp1_mult}  (매도비율 {best_p.tp1_ratio:.0%})"
        )
        log.info(f"    ATR_TP_MULT      = {best_p.atr_tp_mult}")
        log.info(f"    MAX_HOLD_DAYS    = {best_p.max_hold_days_rev}")
        log.info(f"    MAX_HOLD_DAYS_MIX= {best_p.max_hold_days_mix}")
        log.info(f"    MAX_HOLD_DAYS_MOM= {best_p.max_hold_days_mom}")
        log.info(f"    INDEX_DROP_LIMIT = {best_p.max_idx_drop}")
        log.info(f"    ATR_CAP          = {best_p.atr_cap}")
        log.info(
            f"    → {best_s.trades}건  승률 {best_s.win_rate:.1f}%  "
            f"평균 {best_s.avg_ret:+.2f}%  총 {best_s.total_ret:+.1f}%"
        )
    log.info("=" * 100)

    if ranked:
        _check_boundaries(ranked[0][0])


def _check_boundaries(best_p: Params):
    """최적 파라미터가 GRID 경계값에 도달했는지 확인하고 경고."""
    _PARAM_TO_GRID = {
        "invest_min_score": "invest_min_score",
        "atr_sl_mult": "atr_sl_mult",
        "atr_tp_mult": "atr_tp_mult",
        "max_hold_days_rev": "max_hold_days_rev",
        "max_hold_days_mix": "max_hold_days_mix",
        "max_hold_days_mom": "max_hold_days_mom",
        "tp1_mult": "tp1_mult",
        "tp1_ratio": "tp1_ratio",
        "max_idx_drop": "max_idx_drop",
        "atr_cap": "atr_cap",
    }
    for attr, grid_key in _PARAM_TO_GRID.items():
        grid_vals = GRID.get(grid_key)
        if not grid_vals:
            continue
        val = getattr(best_p, attr)
        if val == min(grid_vals):
            log.warning(f"  [BOUNDARY] {grid_key}={val} hits GRID min ({min(grid_vals)}) — consider expanding")
        elif val == max(grid_vals):
            log.warning(f"  [BOUNDARY] {grid_key}={val} hits GRID max ({max(grid_vals)}) — consider expanding")


# ─────────────────────────────────────────
# config.py 자동 갱신
# ─────────────────────────────────────────
_FACTOR_PATH = Path(__file__).resolve().parent.parent / "config.py"


def _params_to_dict(p: Params) -> dict:
    return {
        "INVEST_MIN_SCORE": p.invest_min_score,
        "ATR_SL_MULT": p.atr_sl_mult,
        "ATR_TP_MULT": p.atr_tp_mult,
        "TP1_MULT": p.tp1_mult,
        "TP1_RATIO": p.tp1_ratio,
        "MAX_HOLD_DAYS": p.max_hold_days_rev,
        "MAX_HOLD_DAYS_MIX": p.max_hold_days_mix,
        "MAX_HOLD_DAYS_MOM": p.max_hold_days_mom,
        "INDEX_DROP_LIMIT": p.max_idx_drop,
        "ATR_CAP": p.atr_cap,
    }


def _fmt_val(key: str, val) -> str:
    if isinstance(val, float):
        if val == int(val) and "THRESHOLD" not in key:
            return str(int(val)) if val == 0 else str(val)
        return str(val)
    return str(val)


def _update_config_file(
    best_p: Params, best_s: Stats, start_date: str, end_date: str, elapsed: float,
    best_scores_mom: dict | None = None,
    best_scores_rev: dict | None = None,
):
    """최적 파라미터로 config.py의 Optimized 클래스 속성 갱신."""
    import re
    from datetime import datetime

    d = _params_to_dict(best_p)
    content = _FACTOR_PATH.read_text(encoding="utf-8")

    # 파라미터 갱신
    for k, v in d.items():
        val_str = _fmt_val(k, v)
        type_hint = "int" if isinstance(v, int) else "float"
        content = re.sub(
            rf"^(    {k}: \w+ = )\S+(  # .*)?$",
            lambda m, vs=val_str, th=type_hint, key=k: (
                f"    {key}: {th} = {vs}{m.group(2) or ''}"
            ),
            content,
            flags=re.MULTILINE,
        )

    # 백테스트 결과 주석 갱신
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    elapsed_min = elapsed / 60
    content = re.sub(
        r"^(    # 수행일시:).*$",
        rf"\g<1> {ts} ({elapsed_min:.0f}분)",
        content,
        flags=re.MULTILINE,
    )
    content = re.sub(
        r"^(    # 기간:).*$",
        rf"\g<1> {start_date} ~ {end_date}",
        content,
        flags=re.MULTILINE,
    )
    content = re.sub(
        r"^(    # 성과:).*$",
        rf"\g<1> {best_s.trades}건  승률 {best_s.win_rate:.1f}%  "
        rf"평균 {best_s.avg_ret:+.2f}% (std {best_s.ret_std:.2f}%)  총 {best_s.total_ret:+.1f}%",
        content,
        flags=re.MULTILINE,
    )

    # SIGNAL_SCORES_MOM 갱신 (Optimized 클래스 내부)
    if best_scores_mom:
        mom_str = "    SIGNAL_SCORES_MOM = {\n"
        for k, v in best_scores_mom.items():
            mom_str += f'        "{k}": {v},\n'
        mom_str += "    }"
        content = re.sub(
            r"^    SIGNAL_SCORES_MOM = \{[^}]+\}",
            mom_str,
            content,
            flags=re.MULTILINE | re.DOTALL,
        )

    # SIGNAL_SCORES_REV 갱신 (Optimized 클래스 내부)
    if best_scores_rev:
        rev_str = "    SIGNAL_SCORES_REV = {\n"
        for k, v in best_scores_rev.items():
            rev_str += f'        "{k}": {v},\n'
        rev_str += "    }"
        content = re.sub(
            r"^    SIGNAL_SCORES_REV = \{[^}]+\}",
            rev_str,
            content,
            flags=re.MULTILINE | re.DOTALL,
        )

    _FACTOR_PATH.write_text(content, encoding="utf-8")
    log.info(f"\n  💾 config.py 자동 갱신: {_FACTOR_PATH}")
    log.info(
        f"    {best_s.trades}건  승률 {best_s.win_rate:.1f}%  "
        f"평균 {best_s.avg_ret:+.2f}%  총 {best_s.total_ret:+.1f}%"
    )


# ─────────────────────────────────────────
# 메인
# ─────────────────────────────────────────
def run(
    years: int = 2,
    rebuild: bool = False,
    apply: bool = True,
    workers: int = 0,
):
    global _WORKER_CACHE

    end_date = dtutils.today()
    start_date = dtutils.add_days(end_date, -years * 365)

    log.info(f"최적화 기간: {start_date} ~ {end_date} ({years}년)")

    # 캐시 빌드/로드
    if rebuild:
        for p in CACHE_DIR.glob("opt_cache*.pkl"):
            p.unlink()
            log.info(f"캐시 삭제: {p}")

    cache = build_or_load(start_date, end_date)
    cache.build_arrays()  # numba sim용 numpy 배열 변환
    cache.precompute_signals(min_score=_SCORE_MIN_SCORE)
    cache.flatten_signals()

    # numba 캐시 무효화 (서명 변경 시 필요)
    import shutil
    _nb_cache = Path(__file__).resolve().parent.parent / "__pycache__"
    for _f in _nb_cache.glob("_sim_core.*.nbi"):
        _f.unlink(missing_ok=True)
    for _f in _nb_cache.glob("_sim_core.*.nbc"):
        _f.unlink(missing_ok=True)

    # numba JIT 워밍업 (fork 전에 컴파일 완료)
    import numpy as _np
    _dummy = _np.ones(2, dtype=_np.float64)
    sim_one_nb(100.0, 90.0, 110.0, 120.0, 0.3, 2.0, 5.0,
               _dummy, _dummy, _dummy, _dummy, 2, 0.002, 0.50)
    _df = _np.ones(1, dtype=_np.float64)
    _di = _np.ones(1, dtype=_np.int64)
    _d2 = _np.ones((1, 1), dtype=_np.float64) * 100.0
    backtest_nb(_df * 100, _df * 5, _di * 10, _di * 0, _di,
                _d2, _d2, _d2, _d2,
                9, 2.0, 1.5, 3.0, 0.3, 7, 2, 3, 0.002,
                _df * 0.0, 1.0,
                0.50,
                _di * 0, 0, 120.0)
    _sv = _np.ones(N_FLAGS, dtype=_np.int64)
    backtest_scores_nb(_di, _di * 0, _di * 0,
                       _df * 100, _df * 5, _di,
                       _d2, _d2, _d2, _d2,
                       _sv, _sv, MOM_MASK, REV_MASK, N_FLAGS,
                       9, 2.0, 1.5, 3.0, 0.3, 7, 2, 3, 0.002,
                       _df * 0.0, 1.0,
                       0.50,
                       _di * 0, 0, 120.0,
                       7)
    log.info("[numba] JIT 컴파일 완료")

    # fork 전에 캐시를 전역 변수로 설정 (CoW — 자식 프로세스에 복사 없이 공유)
    _WORKER_CACHE = cache

    # DB 연결 풀 해제: fork 전 SQLAlchemy 백그라운드 스레드 락 제거 (Linux hang 방지)
    try:
        from wye.blsh.database.query import engine as _db_engine

        _db_engine.dispose()
    except Exception:
        pass

    n_workers = workers if workers > 0 else os.cpu_count()
    log.info(f"병렬 처리: {n_workers}코어")

    keys, combos = _dedup_combos(GRID)
    results, elapsed = _grid_search_pass(keys, combos, n_workers)
    _report(results, elapsed)

    if results and results[0][1].metric > -9999:
        best_p, best_s = results[0]
        if apply:
            _update_config_file(best_p, best_s, start_date, end_date, elapsed)
        else:
            log.info("\n  ⚠️  --no-apply: config.py 갱신 생략")


# ─────────────────────────────────────────
# 교대 최적화 (신호점수 ↔ 매매파라미터 수렴)
# ─────────────────────────────────────────
def _grid_search_pass(keys, combos, n_workers, label=""):
    """GRID 조합 병렬 탐색 공통 루틴. sorted results 반환."""
    log.info(f"\n{'─' * 70}")
    log.info(f"  [2단계{label}] {len(combos):,}개 조합")
    log.info(f"{'─' * 70}")

    results = []
    t0 = time.time()
    chunk = max(10, min(200, len(combos) // (n_workers * 32))) if n_workers > 0 else 10

    with mp.Pool(processes=n_workers) as pool:
        for i, (p, s) in enumerate(
            pool.imap_unordered(
                _backtest_worker, ((keys, c) for c in combos), chunksize=chunk
            )
        ):
            results.append((p, s))
            n = i + 1
            if n % 5000 == 0 or n == len(combos):
                elapsed = time.time() - t0
                log.info(f"  {n:>6d}/{len(combos)}  ({elapsed:.0f}초)")

    elapsed = time.time() - t0
    results.sort(key=lambda x: x[1].metric, reverse=True)
    return results, elapsed


def _dedup_combos(grid):
    """중복 조합 제거."""
    keys = list(grid.keys())
    _first = {k: grid[k][0] for k in keys}
    combos = []
    for c in product(*[grid[k] for k in keys]):
        d = dict(zip(keys, c))
        if d["tp1_ratio"] == 1.0 and d["atr_tp_mult"] != _first["atr_tp_mult"]:
            continue
        if d["tp1_mult"] >= d["atr_tp_mult"] and d["tp1_ratio"] != 1.0:
            continue
        # R/R 최소 비율 제약: eff_tp/SL < 0.5 (= R:R < 1:2) 이면 승률 67%+ 필요하므로 제거
        eff_tp = d["tp1_mult"] if d["tp1_ratio"] == 1.0 \
                 else d["tp1_mult"] * d["tp1_ratio"] + d["atr_tp_mult"] * (1 - d["tp1_ratio"])
        rr = eff_tp / d["atr_sl_mult"]
        if rr < 0.5:
            continue
        # max_idx_drop=1.0 (비활성) 시 임계값 중복 제거 불필요 — 이미 한 값만
        combos.append(c)
    return keys, combos


def _run_param_grid(cache, n_workers):
    """2단계: 매매파라미터 GRID 탐색. (best_params, best_stats) 반환."""
    keys, combos = _dedup_combos(GRID)
    results, elapsed = _grid_search_pass(keys, combos, n_workers)
    _report(results, elapsed)

    if not results or results[0][1].metric <= -9999:
        return None

    return results[0]


def run_alternating(
    years: int = 2,
    rebuild: bool = False,
    apply: bool = True,
    workers: int = 0,
    max_iter: int = 5,
):
    """교대 최적화: 1단계(신호점수/승률) ↔ 2단계(매매파라미터/수익률) 수렴."""
    global _WORKER_CACHE

    end_date = dtutils.today()
    start_date = dtutils.add_days(end_date, -years * 365)
    log.info(f"교대 최적화 기간: {start_date} ~ {end_date} ({years}년)")

    if rebuild:
        for p in CACHE_DIR.glob("opt_cache*.pkl"):
            p.unlink()
            log.info(f"캐시 삭제: {p}")

    cache = build_or_load(start_date, end_date)
    cache.build_arrays()
    cache.precompute_signals()
    cache.flatten_signals()

    import numpy as _np
    _dummy = _np.ones(2, dtype=_np.float64)
    sim_one_nb(100.0, 90.0, 110.0, 120.0, 0.3, 2.0, 5.0,
               _dummy, _dummy, _dummy, _dummy, 2, 0.002, 0.50)
    _df = _np.ones(1, dtype=_np.float64)
    _di = _np.ones(1, dtype=_np.int64)
    _d2 = _np.ones((1, 1), dtype=_np.float64) * 100.0
    backtest_nb(_df * 100, _df * 5, _di * 10, _di * 0, _di,
                _d2, _d2, _d2, _d2,
                9, 2.0, 1.5, 3.0, 0.3, 7, 2, 3, 0.002,
                _df * 0.0, 1.0,
                0.50,
                _di * 0, 0, 120.0)
    _sv = _np.ones(N_FLAGS, dtype=_np.int64)
    backtest_scores_nb(_di, _di * 0, _di * 0,
                       _df * 100, _df * 5, _di,
                       _d2, _d2, _d2, _d2,
                       _sv, _sv, MOM_MASK, REV_MASK, N_FLAGS,
                       9, 2.0, 1.5, 3.0, 0.3, 7, 2, 3, 0.002,
                       _df * 0.0, 1.0,
                       0.50,
                       _di * 0, 0, 120.0,
                       7)
    log.info("[numba] JIT 컴파일 완료")

    _WORKER_CACHE = cache

    try:
        from wye.blsh.database.query import engine as _db_engine
        _db_engine.dispose()
    except Exception:
        pass

    n_workers = workers if workers > 0 else os.cpu_count()
    log.info(f"병렬 처리: {n_workers}코어")

    # 초기값
    from wye.blsh.domestic import config as _f
    current_scores_mom = dict(SIGNAL_SCORES_MOM)
    current_scores_rev = dict(SIGNAL_SCORES_REV)
    current_params = Params(
        invest_min_score=_f.INVEST_MIN_SCORE,
        atr_sl_mult=_f.ATR_SL_MULT,
        atr_tp_mult=_f.ATR_TP_MULT,
        max_hold_days_rev=_f.MAX_HOLD_DAYS,
        max_hold_days_mix=_f.MAX_HOLD_DAYS_MIX,
        max_hold_days_mom=_f.MAX_HOLD_DAYS_MOM,
        tp1_mult=_f.TP1_MULT,
        tp1_ratio=_f.TP1_RATIO,
        max_idx_drop=_f.INDEX_DROP_LIMIT,
        atr_cap=_f.ATR_CAP,
    )

    t_total = time.time()
    best_stats = None
    prev_metric = -9999.0
    # 진동 감지용: (scores_mom, scores_rev, params, stats) 이력
    history: list[tuple[dict, dict, Params, Stats]] = []

    for iteration in range(1, max_iter + 1):
        log.info(f"\n{'=' * 70}")
        log.info(f"  교대 최적화 [{iteration}/{max_iter}]")
        log.info(f"{'=' * 70}")

        score_min = (
            current_params.invest_min_score if iteration > 1
            else _SCORE_MIN_SCORE
        )

        # ── 1단계 MOM: MOM 점수 최적화 (mode_filter=6: MOM+MIX)
        mom_a, _ = optimize_scores(
            cache, current_params, n_workers,
            min_score=score_min,
            current_scores_mom=current_scores_mom,
            current_scores_rev=current_scores_rev,
            score_grid=SCORE_GRID_MOM_A,
            target="mom", mode_filter=6,
            label="MOM-고",
        )
        new_mom, _ = optimize_scores(
            cache, current_params, n_workers,
            min_score=score_min,
            current_scores_mom=mom_a,
            current_scores_rev=current_scores_rev,
            score_grid=SCORE_GRID_MOM_B,
            target="mom", mode_filter=6,
            label="MOM-저+NEU",
        )

        # ── 1단계 REV: REV 점수 최적화 (mode_filter=3: REV+MIX)
        _, rev_main = optimize_scores(
            cache, current_params, n_workers,
            min_score=score_min,
            current_scores_mom=new_mom,
            current_scores_rev=current_scores_rev,
            score_grid=SCORE_GRID_REV,
            target="rev", mode_filter=3,
            label="REV",
        )
        _, new_rev = optimize_scores(
            cache, current_params, n_workers,
            min_score=score_min,
            current_scores_mom=new_mom,
            current_scores_rev=rev_main,
            score_grid=SCORE_GRID_REV_NEU,
            target="rev", mode_filter=3,
            label="REV-NEU",
        )

        # ── 캐시 score 재계산 + flat 배열 동기화
        recalc_cache_scores(cache, new_mom, new_rev)
        cache.update_flat_scores()

        # ── 2단계: 매매파라미터 최적화 (mode_filter=7: 전체)
        result = _run_param_grid(cache, n_workers)
        if result is None:
            log.warning("  2단계 유효 결과 없음 → 중단")
            break
        new_params, best_stats = result
        history.append((new_mom, new_rev, new_params, best_stats))

        # ── 수렴 체크: 완전 일치
        if (new_mom == current_scores_mom and new_rev == current_scores_rev
                and new_params == current_params):
            log.info(f"\n  수렴 완료 (iteration {iteration})")
            current_scores_mom = new_mom
            current_scores_rev = new_rev
            current_params = new_params
            break

        # ── metric 변화율 수렴: 개선폭 < 1% → 실질 수렴
        cur_metric = best_stats.metric
        if prev_metric > 0 and cur_metric > 0:
            improvement = (cur_metric - prev_metric) / prev_metric
            if abs(improvement) < 0.01:
                log.info(
                    f"\n  metric 수렴 (변화 {improvement:+.2%},"
                    f" {prev_metric:.1f} → {cur_metric:.1f})"
                )
                current_scores_mom = new_mom
                current_scores_rev = new_rev
                current_params = new_params
                break
        prev_metric = cur_metric

        # ── 2-cycle 진동 감지: 2회 전과 동일하면 더 나은 쪽 선택 후 종료
        if len(history) >= 3:
            p2_mom, p2_rev, p2_params, p2_stats = history[-3]
            if new_mom == p2_mom and new_rev == p2_rev and new_params == p2_params:
                p1_mom, p1_rev, p1_params, p1_stats = history[-2]
                if best_stats.metric >= p1_stats.metric:
                    log.info(f"\n  진동 감지 → 현재 iteration 선택 (metric {best_stats.metric:.1f})")
                    current_scores_mom = new_mom
                    current_scores_rev = new_rev
                    current_params = new_params
                else:
                    log.info(f"\n  진동 감지 → 이전 iteration 선택 (metric {p1_stats.metric:.1f})")
                    current_scores_mom = p1_mom
                    current_scores_rev = p1_rev
                    current_params = p1_params
                    best_stats = p1_stats
                    recalc_cache_scores(cache, current_scores_mom, current_scores_rev)
                break

        current_scores_mom = new_mom
        current_scores_rev = new_rev
        current_params = new_params

    total_elapsed = time.time() - t_total
    log.info(f"\n교대 최적화 완료: {total_elapsed:.0f}초 ({total_elapsed / 60:.1f}분)")

    if apply and best_stats:
        _update_config_file(
            current_params, best_stats, start_date, end_date, total_elapsed,
            best_scores_mom=current_scores_mom,
            best_scores_rev=current_scores_rev,
        )
    elif not apply:
        log.info("\n  ⚠️  --no-apply: config.py 갱신 생략")


# ─────────────────────────────────────────
# Walk-Forward 검증
# ─────────────────────────────────────────
def _generate_wf_windows(start_date, end_date, train_months=18, val_months=6, step_months=3):
    """롤링 윈도우 생성. (train_start, train_end, val_start, val_end) 리스트 반환."""
    from dateutil.relativedelta import relativedelta
    from datetime import datetime

    fmt = "%Y%m%d"
    cursor = datetime.strptime(start_date, fmt)
    end_dt = datetime.strptime(end_date, fmt)
    windows = []
    while True:
        train_end = cursor + relativedelta(months=train_months) - relativedelta(days=1)
        val_start = train_end + relativedelta(days=1)
        val_end = val_start + relativedelta(months=val_months) - relativedelta(days=1)
        if val_end > end_dt:
            break
        windows.append((
            cursor.strftime(fmt), train_end.strftime(fmt),
            val_start.strftime(fmt), val_end.strftime(fmt),
        ))
        cursor += relativedelta(months=step_months)
    return windows


def _wf_report(results: list[tuple[int, str, str, str, str, Stats, Stats, Params]]):
    """Walk-Forward 검증 리포트 출력."""
    log.info("")
    log.info("=" * 100)
    log.info("  Walk-Forward 검증 결과")
    log.info("=" * 100)
    log.info(
        f"  {'#':>2s}  {'Train Period':<21s}  {'Val Period':<21s}  "
        f"{'AvgRet(T)':>9s}  {'AvgRet(V)':>9s}  {'Ratio':>5s}  Params"
    )
    log.info("-" * 100)

    ratios = []
    overfit_windows = []
    for idx, ts, te, vs, ve, train_st, val_st, best_p in results:
        ratio = val_st.avg_ret / train_st.avg_ret * 100 if train_st.avg_ret > 0 else 0
        ratios.append(ratio)
        warn = " ⚠️" if ratio < 50 else ""
        if ratio < 50:
            overfit_windows.append(idx)
        p_summary = (
            f"SL={best_p.atr_sl_mult:.1f} "
            f"REV={best_p.max_hold_days_rev}d MIX={best_p.max_hold_days_mix}d"
        )
        log.info(
            f"  {idx:2d}  {ts}~{te}  {vs}~{ve}  "
            f"{train_st.avg_ret:>+8.2f}%  {val_st.avg_ret:>+8.2f}%  {ratio:>4.0f}%{warn}  {p_summary}"
        )

    log.info("-" * 100)
    if ratios:
        avg_ratio = sum(ratios) / len(ratios)
        log.info(f"  평균 Val/Train AvgRet 비율: {avg_ratio:.0f}%")
    for w in overfit_windows:
        log.warning(f"  Window {w}: 과적합 의심 (avg_ret 비율 < 50%)")
    log.info("=" * 100)

    # 텔레그램 WF 검증 결과 요약 발송
    try:
        from wye.blsh.common import messageutils
        lines = ["📊 Walk-Forward 검증 결과"]
        for idx, ts, te, vs, ve, train_st, val_st, best_p in results:
            ratio = val_st.avg_ret / train_st.avg_ret * 100 if train_st.avg_ret > 0 else 0
            warn = " ⚠️" if ratio < 50 else ""
            lines.append(
                f"  W{idx}: T={train_st.avg_ret:+.2f}% V={val_st.avg_ret:+.2f}% ({ratio:.0f}%){warn}"
            )
        if ratios:
            lines.append(f"  평균 비율: {avg_ratio:.0f}%")
        if overfit_windows:
            lines.append(f"  🚨 과적합 의심: W{', W'.join(str(w) for w in overfit_windows)}")
        messageutils.send_message("\n".join(lines))
    except Exception:
        pass  # 텔레그램 실패 시 무시


def _wf_detail_report(w_idx: int, trades: list[TradeRecord]):
    """WF 윈도우별 상세 분석: mode/market/flag 조합별 성과."""
    from collections import Counter

    if not trades:
        log.info(f"\n  [Window {w_idx}] 거래 없음")
        return

    log.info(f"\n  [Window {w_idx}] 상세 분석 ({len(trades)}건)")

    # ── Mode breakdown
    log.info(f"  {'Mode':<5} {'거래':>5} {'승률':>7} {'평균수익':>9}")
    log.info(f"  {'-' * 30}")
    for mode in ("REV", "MOM", "MIX"):
        mode_trades = [t for t in trades if t.mode == mode]
        if not mode_trades:
            continue
        n = len(mode_trades)
        wins = sum(1 for t in mode_trades if t.result not in ("SL", "HOLD"))
        avg_ret = sum(t.ret_pct for t in mode_trades) / n
        wr = 100 * wins / n
        log.info(f"  {mode:<5} {n:>5} {wr:>6.1f}% {avg_ret:>+8.2f}%")

    # ── Market breakdown
    log.info(f"  {'Market':<8} {'거래':>5} {'승률':>7} {'평균수익':>9}")
    log.info(f"  {'-' * 33}")
    markets = sorted(set(t.market for t in trades))
    for market in markets:
        mkt_trades = [t for t in trades if t.market == market]
        n = len(mkt_trades)
        wins = sum(1 for t in mkt_trades if t.result not in ("SL", "HOLD"))
        avg_ret = sum(t.ret_pct for t in mkt_trades) / n
        wr = 100 * wins / n
        log.info(f"  {market:<8} {n:>5} {wr:>6.1f}% {avg_ret:>+8.2f}%")

    # ── Top 5 flag combinations
    _SUPPLY = {"F_TRN", "I_TRN", "F_C3", "I_C3", "F_1", "I_1", "FI", "P_OV"}
    combo_stats: dict[str, list[TradeRecord]] = {}
    for t in trades:
        flags = sorted(f for f in t.flags.split(",") if f and f not in _SUPPLY)
        key = "+".join(flags) if flags else "(none)"
        combo_stats.setdefault(key, []).append(t)

    sorted_combos = sorted(combo_stats.items(), key=lambda x: len(x[1]), reverse=True)[:5]
    log.info(f"  {'Flag조합':<25} {'거래':>5} {'승률':>7} {'평균수익':>9}")
    log.info(f"  {'-' * 50}")
    for combo, ctrades in sorted_combos:
        n = len(ctrades)
        wins = sum(1 for t in ctrades if t.result not in ("SL", "HOLD"))
        wr = 100 * wins / n
        avg_r = sum(t.ret_pct for t in ctrades) / n
        label = combo[:25]
        log.info(f"  {label:<25} {n:>5} {wr:>6.1f}% {avg_r:>+8.2f}%")


def run_walkforward(
    years: int = 2,
    rebuild: bool = False,
    workers: int = 0,
    train_months: int = 18,
    val_months: int = 6,
    step_months: int = 3,
    detail: bool = False,
):
    """Walk-Forward 검증: 롤링 윈도우별 train 최적화 → val 검증.

    현재 config.py의 SIGNAL_SCORES_MOM/REV를 고정하고, Stage 2(매매 파라미터)만
    각 train 윈도우에서 최적화한 뒤 val 윈도우에서 검증.
    """
    global _WORKER_CACHE

    end_date = dtutils.today()
    start_date = dtutils.add_days(end_date, -years * 365)
    log.info(f"Walk-Forward 검증 기간: {start_date} ~ {end_date} ({years}년)")
    log.info(f"  Train={train_months}개월  Val={val_months}개월  Step={step_months}개월")

    windows = _generate_wf_windows(start_date, end_date, train_months, val_months, step_months)
    if not windows:
        log.error("유효한 윈도우가 없습니다. 기간을 늘리거나 train/val 개월 수를 줄이세요.")
        return
    log.info(f"  윈도우 수: {len(windows)}")

    if rebuild:
        for p in CACHE_DIR.glob("opt_cache*.pkl"):
            p.unlink()
            log.info(f"캐시 삭제: {p}")

    cache = build_or_load(start_date, end_date)
    cache.build_arrays()
    cache.precompute_signals()
    cache.flatten_signals()

    # numba JIT 워밍업
    import numpy as _np
    _dummy = _np.ones(2, dtype=_np.float64)
    sim_one_nb(100.0, 90.0, 110.0, 120.0, 0.3, 2.0, 5.0,
               _dummy, _dummy, _dummy, _dummy, 2, 0.002, 0.50)
    _df = _np.ones(1, dtype=_np.float64)
    _di = _np.ones(1, dtype=_np.int64)
    _d2 = _np.ones((1, 1), dtype=_np.float64) * 100.0
    backtest_nb(_df * 100, _df * 5, _di * 10, _di * 0, _di,
                _d2, _d2, _d2, _d2,
                9, 2.0, 1.5, 3.0, 0.3, 7, 2, 3, 0.002,
                _df * 0.0, 1.0,
                0.50,
                _di * 0, 0, 120.0)
    log.info("[numba] JIT 컴파일 완료")

    try:
        from wye.blsh.database.query import engine as _db_engine
        _db_engine.dispose()
    except Exception:
        pass

    n_workers = workers if workers > 0 else os.cpu_count()
    log.info(f"병렬 처리: {n_workers}코어")

    t_total = time.time()
    results = []

    for w_idx, (ts, te, vs, ve) in enumerate(windows, 1):
        log.info(f"\n{'=' * 70}")
        log.info(f"  Window {w_idx}/{len(windows)}: Train {ts}~{te}  Val {vs}~{ve}")
        log.info(f"{'=' * 70}")

        # Train
        train_cache = cache.slice_by_dates(ts, te)
        train_cache.flatten_signals()
        _WORKER_CACHE = train_cache

        result = _run_param_grid(train_cache, n_workers)
        if result is None:
            log.warning(f"  Window {w_idx}: train 유효 결과 없음 → 스킵")
            continue
        best_p, train_st = result

        if train_st.trades < 30:
            log.warning(f"  Window {w_idx}: train 거래 {train_st.trades}건 < 30 → 스킵")
            continue

        # Validation
        val_cache = cache.slice_by_dates(vs, ve)
        val_cache.flatten_signals()

        if detail:
            val_st, val_trades = backtest_with_trades(val_cache, best_p)
        else:
            val_st = backtest(val_cache, best_p)

        if val_st.trades < 30:
            log.warning(f"  Window {w_idx}: val 거래 {val_st.trades}건 < 30 → 스킵")
            continue

        log.info(
            f"  Train: {train_st.trades}건  승률 {train_st.win_rate:.1f}%  평균 {train_st.avg_ret:+.2f}%"
        )
        log.info(
            f"  Val:   {val_st.trades}건  승률 {val_st.win_rate:.1f}%  평균 {val_st.avg_ret:+.2f}%"
        )

        if detail:
            _wf_detail_report(w_idx, val_trades)

        results.append((w_idx, ts, te, vs, ve, train_st, val_st, best_p))

    total_elapsed = time.time() - t_total
    log.info(f"\nWalk-Forward 검증 완료: {total_elapsed:.0f}초 ({total_elapsed / 60:.1f}분)")

    if results:
        _wf_report(results)
    else:
        log.warning("유효한 윈도우 결과가 없습니다.")


# ─────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="Factor 최적화 Grid Search")
    parser.add_argument("--years", type=float, default=2, help="백테스트 기간 (년, 소수 가능)")
    parser.add_argument("--rebuild", action="store_true", help="캐시 강제 재빌드")
    parser.add_argument(
        "--no-apply", action="store_true", help="config.py 자동 갱신 생략"
    )
    parser.add_argument(
        "--workers", type=int, default=0, help="병렬 프로세스 수 (0=자동)"
    )
    parser.add_argument(
        "--alternating", action="store_true",
        help="교대 최적화 (신호점수 ↔ 매매파라미터 수렴)"
    )
    parser.add_argument(
        "--max-iter", type=int, default=5, help="교대 최적화 최대 반복 횟수"
    )
    parser.add_argument(
        "--walkforward", action="store_true",
        help="Walk-Forward 검증 (롤링 윈도우별 train→val)"
    )
    parser.add_argument("--train-months", type=int, default=18, help="WF 학습 기간 (개월)")
    parser.add_argument("--val-months", type=int, default=6, help="WF 검증 기간 (개월)")
    parser.add_argument("--step-months", type=int, default=3, help="WF 롤링 간격 (개월)")
    parser.add_argument(
        "--detail", action="store_true",
        help="Walk-Forward 상세 리포트 (val 윈도우별 mode/market/flag 분석)"
    )
    args = parser.parse_args()

    if args.alternating and args.walkforward:
        parser.error("--alternating과 --walkforward는 동시 지정 불가")

    if args.walkforward:
        run_walkforward(
            years=args.years,
            rebuild=args.rebuild,
            workers=args.workers,
            train_months=args.train_months,
            val_months=args.val_months,
            step_months=args.step_months,
            detail=args.detail,
        )
    elif args.alternating:
        run_alternating(
            years=args.years,
            rebuild=args.rebuild,
            apply=not args.no_apply,
            workers=args.workers,
            max_iter=args.max_iter,
        )
    else:
        run(
            years=args.years,
            rebuild=args.rebuild,
            apply=not args.no_apply,
            workers=args.workers,
        )
