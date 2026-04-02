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
from wye.blsh.domestic.config import SELL_COST_RATE, SIGNAL_SCORES, SUPPLY_CAP
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
    sector_penalty_threshold: float  # 업종지수 MA20 괴리율 패널티 임계값 (e.g. -0.03)
    sector_penalty_pts: int  # 임계값 이하 시 점수 패널티 (e.g. -2)
    sector_bonus_threshold: float  # 업종지수 MA20 괴리율 보너스 임계값 (e.g. 0.0)
    sector_bonus_pts: int  # 임계값 이상 시 보너스 (e.g. +1)

    def label(self) -> str:
        parts = []
        if self.sector_penalty_pts != 0:
            parts.append(
                f"pen={self.sector_penalty_threshold:.0%}/{self.sector_penalty_pts:+d}"
            )
        if self.sector_bonus_pts != 0:
            parts.append(
                f"bon={self.sector_bonus_threshold:+.0%}/{self.sector_bonus_pts:+d}"
            )
        sec = " ".join(parts) if parts else "sec=off"
        return (
            f"score≥{self.invest_min_score} "
            f"SL={self.atr_sl_mult:.1f} TP1={self.tp1_mult:.1f}({self.tp1_ratio:.0%}) "
            f"TP2={self.atr_tp_mult:.1f} "
            f"REV={self.max_hold_days_rev}d MIX={self.max_hold_days_mix}d "
            f"MOM={self.max_hold_days_mom}d {sec}".rstrip()
        )


@dataclass
class Stats:
    trades: int = 0
    wins: int = 0
    losses: int = 0
    holds: int = 0
    total_ret: float = 0.0

    @property
    def win_rate(self) -> float:
        return self.wins / self.trades * 100 if self.trades else 0

    @property
    def avg_ret(self) -> float:
        return self.total_ret / self.trades if self.trades else 0

    @property
    def metric(self) -> float:
        """최적화 지표: 총수익 × min(1, trades/100). 거래 30건 미만 패널티."""
        if self.trades < 30:
            return -9999
        return self.total_ret * min(1.0, self.trades / 100)


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
        sl = float(floor_tick_nb(buy - params.atr_sl_mult * atr))
        tp1 = float(ceil_tick_nb(buy + params.tp1_mult * atr))
        tp2 = float(ceil_tick_nb(buy + params.atr_tp_mult * atr))

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
    sl = Tick.floor_tick(buy - params.atr_sl_mult * atr)
    tp1 = Tick.ceil_tick(buy + params.tp1_mult * atr)
    tp2 = Tick.ceil_tick(buy + params.atr_tp_mult * atr)

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
        trades, wins, losses, holds, total_ret = backtest_nb(
            cache.flat_buy, cache.flat_atr, cache.flat_score,
            cache.flat_sec_gap, cache.flat_mode_id, cache.flat_n_bars,
            cache.flat_opens, cache.flat_highs, cache.flat_lows, cache.flat_closes,
            params.invest_min_score,
            params.atr_sl_mult, params.tp1_mult, params.atr_tp_mult,
            params.tp1_ratio,
            params.max_hold_days_rev, params.max_hold_days_mix, params.max_hold_days_mom,
            params.sector_penalty_pts, params.sector_penalty_threshold,
            params.sector_bonus_pts, params.sector_bonus_threshold,
            SELL_COST_RATE,
        )
        st = Stats()
        st.trades = trades
        st.wins = wins
        st.losses = losses
        st.holds = holds
        st.total_ret = total_ret
        return st

    # ── fallback: dict 경로
    st = Stats()
    _max_hold = (params.max_hold_days_rev, params.max_hold_days_mix, params.max_hold_days_mom)
    _pen_pts = params.sector_penalty_pts
    _pen_th = params.sector_penalty_threshold
    _bon_pts = params.sector_bonus_pts
    _bon_th = params.sector_bonus_threshold
    _min_score = params.invest_min_score
    _sl_mult = params.atr_sl_mult
    _tp1_mult = params.tp1_mult
    _tp2_mult = params.atr_tp_mult
    _tp1_ratio = params.tp1_ratio
    _cost = SELL_COST_RATE

    for base_date in cache.scan_dates:
        sigs = cache.signals.get(base_date)
        if not sigs:
            continue

        for sig in sigs:
            if sig.get("_skip", True):
                continue

            effective_score = sig["score"]
            sec_gap = sig.get("sector_gap", 0.0)
            if _pen_pts != 0 and sec_gap < _pen_th:
                effective_score += _pen_pts
            elif _bon_pts != 0 and sec_gap >= _bon_th:
                effective_score += _bon_pts

            if effective_score < _min_score:
                continue

            buy = sig["_buy"]
            atr = sig["atr"]
            sl = float(floor_tick_nb(buy - _sl_mult * atr))
            tp1 = float(ceil_tick_nb(buy + _tp1_mult * atr))
            tp2 = float(ceil_tick_nb(buy + _tp2_mult * atr))

            max_d = _max_hold[sig["_mode_id"]]
            n = min(max_d + 1, sig["_n_bars"])

            res_id, ret_pct, _, _ = sim_one_nb(
                buy, sl, tp1, tp2, _tp1_ratio, _sl_mult, atr,
                sig["_opens"], sig["_highs"], sig["_lows"], sig["_closes"],
                n, _cost,
            )

            st.trades += 1
            st.total_ret += ret_pct
            if res_id <= 3:
                if res_id == 0:
                    st.losses += 1
                else:
                    st.wins += 1
            else:
                st.holds += 1

    return st


# ─────────────────────────────────────────
# 1단계: 신호 점수 최적화 (승률 기준)
# ─────────────────────────────────────────
SCORE_GRID_A = {
    # 고배점 신호 (6개): 0~3 탐색 (0 = 비활성화)
    "MGC": [0, 1, 2, 3],
    "W52": [0, 1, 2, 3],
    "PB":  [0, 1, 2, 3],
    "LB":  [0, 1, 2, 3],
    "MS":  [0, 1, 2, 3],
    "RBO": [0, 1, 2, 3],
}  # 4^6 = 4,096 콤보

SCORE_GRID_B = {
    # 저배점 신호 (9개): 0~2 탐색
    "MPGC": [0, 1, 2],
    "ROV":  [0, 1, 2],
    "BBL":  [0, 1, 2],
    "BBM":  [0, 1, 2],
    "VS":   [0, 1, 2],
    "MAA":  [0, 1, 2],
    "SGC":  [0, 1, 2],
    "HMR":  [0, 1, 2],
    "OBV":  [0, 1, 2],
}  # 3^9 = 19,683 콤보

SCORE_GRID = SCORE_GRID_A  # 하위 호환


def _calc_score_with(flags: set, mode: str, scores: dict) -> int:
    """scores dict로 tech_score 재계산."""
    mom = sum(scores.get(f, 0) for f in flags & _MOMENTUM_FLAGS)
    rev = sum(scores.get(f, 0) for f in flags & _REVERSAL_FLAGS)
    neu = sum(scores.get(f, 0) for f in flags - _ALL_FLAGS)
    if mode == "MOM":
        return mom + neu
    if mode == "REV":
        return rev + neu
    if mode == "MIX":
        return max(mom, rev) + neu
    return mom + rev + neu


def backtest_scores(
    cache: OptCache, params: Params, scores: dict, min_score: int,
) -> Stats:
    """SIGNAL_SCORES를 변경하여 백테스트 (사전 계산된 OHLCV 사용)."""
    st = Stats()
    _MODE_MAP = {"MOM": 2, "MIX": 1, "REV": 0}
    _max_hold = (params.max_hold_days_rev, params.max_hold_days_mix, params.max_hold_days_mom)
    _pen_pts = params.sector_penalty_pts
    _pen_th = params.sector_penalty_threshold
    _bon_pts = params.sector_bonus_pts
    _bon_th = params.sector_bonus_threshold
    _sl_mult = params.atr_sl_mult
    _tp1_mult = params.tp1_mult
    _tp2_mult = params.atr_tp_mult
    _tp1_ratio = params.tp1_ratio
    _cost = SELL_COST_RATE

    for base_date in cache.scan_dates:
        sigs = cache.signals.get(base_date)
        if not sigs:
            continue

        for sig in sigs:
            if sig.get("_skip", True):
                continue

            # 신호 점수 재계산
            flag_str = sig.get("flags", "")
            flags = set(flag_str.split(",")) if flag_str else set()
            flags.discard("")
            mode = _classify_mode(flags)
            if mode not in _MODE_MAP:
                continue

            tech_score = _calc_score_with(flags, mode, scores)
            supply_bonus = min(sig.get("raw_supply_bonus", 0), SUPPLY_CAP)
            effective_score = tech_score + supply_bonus
            if "P_OV" in flags:
                effective_score -= 1

            sec_gap = sig.get("sector_gap", 0.0)
            if _pen_pts != 0 and sec_gap < _pen_th:
                effective_score += _pen_pts
            elif _bon_pts != 0 and sec_gap >= _bon_th:
                effective_score += _bon_pts

            if effective_score < min_score:
                continue

            buy = sig["_buy"]
            atr = sig["atr"]
            sl = float(floor_tick_nb(buy - _sl_mult * atr))
            tp1 = float(ceil_tick_nb(buy + _tp1_mult * atr))
            tp2 = float(ceil_tick_nb(buy + _tp2_mult * atr))

            max_d = _max_hold[_MODE_MAP[mode]]
            n = min(max_d + 1, sig["_n_bars"])

            res_id, ret_pct, _, _ = sim_one_nb(
                buy, sl, tp1, tp2, _tp1_ratio, _sl_mult, atr,
                sig["_opens"], sig["_highs"], sig["_lows"], sig["_closes"],
                n, _cost,
            )

            st.trades += 1
            st.total_ret += ret_pct
            if res_id == 0:
                st.losses += 1
            elif res_id <= 3:
                st.wins += 1
            else:
                st.holds += 1

    return st


def _score_worker(args):
    """1단계 워커: (score_keys, score_combo, params_dict, base_scores)."""
    score_keys, score_combo, params_dict, base_scores = args
    scores = {**base_scores, **dict(zip(score_keys, score_combo))}
    params = Params(**params_dict)

    cache = _WORKER_CACHE
    if hasattr(cache, "flat_flag_mask") and cache.flat_flag_mask is not None:
        import numpy as _np
        sv = _np.array([scores.get(f, 0) for f in FLAG_ORDER], dtype=_np.int64)
        trades, wins, losses, holds, total_ret = backtest_scores_nb(
            cache.flat_flag_mask, cache.flat_supply_bonus, cache.flat_has_pov,
            cache.flat_buy, cache.flat_atr, cache.flat_sec_gap, cache.flat_n_bars,
            cache.flat_opens, cache.flat_highs, cache.flat_lows, cache.flat_closes,
            sv, MOM_MASK, REV_MASK, N_FLAGS,
            _WORKER_MIN_SCORE,
            params.atr_sl_mult, params.tp1_mult, params.atr_tp_mult,
            params.tp1_ratio,
            params.max_hold_days_rev, params.max_hold_days_mix, params.max_hold_days_mom,
            params.sector_penalty_pts, params.sector_penalty_threshold,
            params.sector_bonus_pts, params.sector_bonus_threshold,
            SELL_COST_RATE,
        )
        st = Stats()
        st.trades = trades
        st.wins = wins
        st.losses = losses
        st.holds = holds
        st.total_ret = total_ret
    else:
        st = backtest_scores(cache, params, scores, _WORKER_MIN_SCORE)

    return scores, st


def optimize_scores(
    cache: OptCache, params: Params, n_workers: int,
    min_score: int | None = None,
    current_scores: dict | None = None,
    score_grid: dict | None = None,
    label: str = "",
) -> dict:
    """신호 점수 그리드 탐색. best_scores (전체 15개 신호) 반환.

    Args:
        min_score: 진입 최소 점수. None이면 GRID 최소값 사용.
        current_scores: 전체 신호의 현재 점수. None이면 SIGNAL_SCORES 초기값.
        score_grid: 탐색 대상 그리드. None이면 SCORE_GRID_A.
        label: 로그 라벨 (e.g. "A: 고배점", "B: 저배점").
    """
    global _WORKER_MIN_SCORE
    _min = min_score if min_score is not None else _SCORE_MIN_SCORE
    _WORKER_MIN_SCORE = _min

    grid = score_grid if score_grid is not None else SCORE_GRID_A
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
        "sector_penalty_threshold": params.sector_penalty_threshold,
        "sector_penalty_pts": params.sector_penalty_pts,
        "sector_bonus_threshold": params.sector_bonus_threshold,
        "sector_bonus_pts": params.sector_bonus_pts,
    }

    # 탐색 대상 외 신호: current_scores에서 고정
    src = current_scores if current_scores is not None else dict(SIGNAL_SCORES)
    base_scores = {k: v for k, v in src.items() if k not in grid}

    tasks = [
        (score_keys, combo, params_dict, base_scores)
        for combo in all_score_combos
    ]
    tag = f" {label}" if label else ""
    log.info(f"\n{'─' * 70}")
    log.info(
        f"  [1단계{tag}] 신호 점수 최적화: {len(tasks):,}개 조합"
        f"  (min_score={_min})"
    )
    log.info(f"{'─' * 70}")

    results: list[tuple[dict, Stats]] = []
    t0 = time.time()
    chunk = max(10, min(200, len(tasks) // (n_workers * 32)))

    with mp.Pool(processes=n_workers) as pool:
        for i, (scores, st) in enumerate(
            pool.imap_unordered(_score_worker, tasks, chunksize=chunk)
        ):
            results.append((scores, st))
            n = i + 1
            if n % 500 == 0 or n == len(tasks):
                elapsed = time.time() - t0
                log.info(f"  {n:>6d}/{len(tasks)}  ({elapsed:.0f}초)")

    results.sort(key=lambda x: x[1].metric, reverse=True)

    elapsed = time.time() - t0
    if results and results[0][1].metric > -9999:
        best_scores, best_st = results[0]
        log.info(
            f"\n  ★ 1단계{tag} 최적:"
            f"  {best_st.trades}건  승률 {best_st.win_rate:.1f}%"
            f"  평균 {best_st.avg_ret:+.2f}%  총 {best_st.total_ret:+.1f}%"
            f"  [{elapsed:.0f}초]"
        )
        changed = {k: v for k, v in best_scores.items() if v != src.get(k)}
        if changed:
            log.info(f"    변경: {changed}")
        return best_scores
    log.warning(f"  1단계{tag}: 유효 결과 없음 → 기존 값 유지")
    return dict(src)


def recalc_cache_scores(cache: OptCache, scores: dict):
    """캐시 signal의 score를 새 SIGNAL_SCORES로 재계산."""
    for date, sigs in cache.signals.items():
        for sig in sigs:
            flag_str = sig.get("flags", "")
            flags = set(flag_str.split(",")) if flag_str else set()
            flags.discard("")
            mode = _classify_mode(flags)
            sig["mode"] = mode
            tech_score = _calc_score_with(flags, mode, scores)
            sig["tech_score"] = tech_score
            supply_bonus = min(sig.get("raw_supply_bonus", 0), SUPPLY_CAP)
            score = tech_score + supply_bonus
            if "P_OV" in flags:
                score -= 1
            sig["score"] = score


# ─────────────────────────────────────────
# 그리드 정의
# ─────────────────────────────────────────
GRID = {
    "invest_min_score": [9, 10, 11, 12, 13],
    "atr_sl_mult": [1.0, 1.5, 2.0, 2.5, 3.0],
    "atr_tp_mult": [1.5, 2.0, 2.5, 3.0, 4.0, 5.0],
    "max_hold_days_rev": [3, 5, 7, 10],
    "max_hold_days_mix": [2, 3, 5],
    "max_hold_days_mom": [1, 2, 3],
    "tp1_mult": [0.7, 1.0, 1.5],
    "tp1_ratio": [0.3, 0.5, 0.7, 1.0],
    "sector_penalty_threshold": [-0.03, -0.05],
    "sector_penalty_pts": [0, -2],
    "sector_bonus_threshold": [0.0, 0.02],
    "sector_bonus_pts": [0, 1],
}  # 5×5×6×4×3×3×3×4×2×2×2×2 = 1,036,800 → --no-sector 시 64,800

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
        f"  {'#':>3s}  {'거래':>6s}  {'승률':>6s}  {'평균수익':>8s}  {'총수익':>10s}  │ 파라미터"
    )
    log.info("-" * 100)

    for rank, (p, s) in enumerate(ranked[:15], 1):
        log.info(
            f"  {rank:3d}  {s.trades:>5d}건  {s.win_rate:>5.1f}%  "
            f"{s.avg_ret:>+7.2f}%  {s.total_ret:>+9.1f}%  │ {p.label()}"
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
        sec_parts = []
        if best_p.sector_penalty_pts != 0:
            sec_parts.append(
                f"패널티: 업종MA20괴리<{best_p.sector_penalty_threshold:.0%} → {best_p.sector_penalty_pts:+d}점"
            )
        if best_p.sector_bonus_pts != 0:
            sec_parts.append(
                f"보너스: 업종MA20괴리≥{best_p.sector_bonus_threshold:.0%} → {best_p.sector_bonus_pts:+d}점"
            )
        log.info(
            f"    SECTOR_ADJUST    = {', '.join(sec_parts) if sec_parts else 'off'}"
        )
        log.info(
            f"    → {best_s.trades}건  승률 {best_s.win_rate:.1f}%  "
            f"평균 {best_s.avg_ret:+.2f}%  총 {best_s.total_ret:+.1f}%"
        )
    log.info("=" * 100)


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
        "SECTOR_PENALTY_THRESHOLD": p.sector_penalty_threshold,
        "SECTOR_PENALTY_PTS": p.sector_penalty_pts,
        "SECTOR_BONUS_THRESHOLD": p.sector_bonus_threshold,
        "SECTOR_BONUS_PTS": p.sector_bonus_pts,
    }


def _fmt_val(key: str, val) -> str:
    if isinstance(val, float):
        if val == int(val) and "THRESHOLD" not in key:
            return str(int(val)) if val == 0 else str(val)
        return str(val)
    return str(val)


def _update_config_file(
    best_p: Params, best_s: Stats, start_date: str, end_date: str, elapsed: float,
    best_scores: dict | None = None,
):
    """최적 파라미터로 config.py의 Optimized 클래스 속성 갱신. best_scores 지정 시 SIGNAL_SCORES도 갱신."""
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
        rf"평균 {best_s.avg_ret:+.2f}%  총 {best_s.total_ret:+.1f}%",
        content,
        flags=re.MULTILINE,
    )

    # SIGNAL_SCORES 갱신
    if best_scores:
        scores_str = "SIGNAL_SCORES = {\n"
        for k, v in best_scores.items():
            scores_str += f'    "{k}": {v},\n'
        scores_str += "}"
        content = re.sub(
            r"^SIGNAL_SCORES = \{[^}]+\}",
            scores_str,
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
    sector: bool = True,
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
    cache.precompute_signals(
        min_score=_SCORE_MIN_SCORE,
        max_sector_bonus=max(GRID.get("sector_bonus_pts", [0])),
    )
    cache.flatten_signals()

    # numba JIT 워밍업 (fork 전에 컴파일 완료)
    import numpy as _np
    _dummy = _np.ones(2, dtype=_np.float64)
    sim_one_nb(100.0, 90.0, 110.0, 120.0, 0.3, 2.0, 5.0,
               _dummy, _dummy, _dummy, _dummy, 2, 0.002)
    _df = _np.ones(1, dtype=_np.float64)
    _di = _np.ones(1, dtype=_np.int64)
    _d2 = _np.ones((1, 1), dtype=_np.float64) * 100.0
    backtest_nb(_df * 100, _df * 5, _di * 10, _df, _di * 0, _di,
                _d2, _d2, _d2, _d2,
                9, 2.0, 1.5, 3.0, 0.3, 7, 2, 3, 0, -0.03, 0, 0.0, 0.002)
    _sv = _np.ones(N_FLAGS, dtype=_np.int64)
    backtest_scores_nb(_di, _di * 0, _di * 0,
                       _df * 100, _df * 5, _df, _di,
                       _d2, _d2, _d2, _d2,
                       _sv, MOM_MASK, REV_MASK, N_FLAGS,
                       9, 2.0, 1.5, 3.0, 0.3, 7, 2, 3, 0, -0.03, 0, 0.0, 0.002)
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

    grid = GRID.copy()
    if not sector:
        grid.update({
            "sector_penalty_threshold": [-0.03],
            "sector_penalty_pts": [0],
            "sector_bonus_threshold": [0.0],
            "sector_bonus_pts": [0],
        })

    keys, combos = _dedup_combos(grid)
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
        if d["sector_penalty_pts"] == 0 and d["sector_penalty_threshold"] != _first["sector_penalty_threshold"]:
            continue
        if d["sector_bonus_pts"] == 0 and d["sector_bonus_threshold"] != _first["sector_bonus_threshold"]:
            continue
        combos.append(c)
    return keys, combos


def _run_param_grid(cache, n_workers, sector=True):
    """2단계: 매매파라미터 GRID 탐색 (2-pass). (best_params, best_stats) 반환."""
    # Pass 1: 핵심 파라미터만 탐색 (sector OFF)
    grid_core = GRID.copy()
    grid_core.update({
        "sector_penalty_threshold": [-0.03],
        "sector_penalty_pts": [0],
        "sector_bonus_threshold": [0.0],
        "sector_bonus_pts": [0],
    })
    keys, combos = _dedup_combos(grid_core)
    results, elapsed = _grid_search_pass(keys, combos, n_workers, " Pass1: 핵심파라미터")
    _report(results, elapsed)

    if not results or results[0][1].metric <= -9999:
        return None

    best_p, best_s = results[0]

    if not sector:
        return best_p, best_s

    # Pass 2: 핵심 파라미터 고정 + sector만 탐색
    grid_sec = {
        "invest_min_score": [best_p.invest_min_score],
        "atr_sl_mult": [best_p.atr_sl_mult],
        "atr_tp_mult": [best_p.atr_tp_mult],
        "max_hold_days_rev": [best_p.max_hold_days_rev],
        "max_hold_days_mix": [best_p.max_hold_days_mix],
        "max_hold_days_mom": [best_p.max_hold_days_mom],
        "tp1_mult": [best_p.tp1_mult],
        "tp1_ratio": [best_p.tp1_ratio],
        "sector_penalty_threshold": GRID["sector_penalty_threshold"],
        "sector_penalty_pts": GRID["sector_penalty_pts"],
        "sector_bonus_threshold": GRID["sector_bonus_threshold"],
        "sector_bonus_pts": GRID["sector_bonus_pts"],
    }
    keys_sec, combos_sec = _dedup_combos(grid_sec)
    results_sec, elapsed_sec = _grid_search_pass(
        keys_sec, combos_sec, n_workers, " Pass2: sector"
    )

    if results_sec and results_sec[0][1].metric > best_s.metric:
        best_p, best_s = results_sec[0]
        log.info(
            f"  sector 적용 개선: {best_s.trades}건  "
            f"총 {best_s.total_ret:+.1f}%  ({best_p.label()})"
        )
    else:
        log.info(f"  sector 미적용이 최적")

    return best_p, best_s


def run_alternating(
    years: int = 2,
    rebuild: bool = False,
    sector: bool = True,
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
               _dummy, _dummy, _dummy, _dummy, 2, 0.002)
    _df = _np.ones(1, dtype=_np.float64)
    _di = _np.ones(1, dtype=_np.int64)
    _d2 = _np.ones((1, 1), dtype=_np.float64) * 100.0
    backtest_nb(_df * 100, _df * 5, _di * 10, _df, _di * 0, _di,
                _d2, _d2, _d2, _d2,
                9, 2.0, 1.5, 3.0, 0.3, 7, 2, 3, 0, -0.03, 0, 0.0, 0.002)
    _sv = _np.ones(N_FLAGS, dtype=_np.int64)
    backtest_scores_nb(_di, _di * 0, _di * 0,
                       _df * 100, _df * 5, _df, _di,
                       _d2, _d2, _d2, _d2,
                       _sv, MOM_MASK, REV_MASK, N_FLAGS,
                       9, 2.0, 1.5, 3.0, 0.3, 7, 2, 3, 0, -0.03, 0, 0.0, 0.002)
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
    current_scores = dict(SIGNAL_SCORES)
    current_params = Params(
        invest_min_score=_f.INVEST_MIN_SCORE,
        atr_sl_mult=_f.ATR_SL_MULT,
        atr_tp_mult=_f.ATR_TP_MULT,
        max_hold_days_rev=_f.MAX_HOLD_DAYS,
        max_hold_days_mix=_f.MAX_HOLD_DAYS_MIX,
        max_hold_days_mom=_f.MAX_HOLD_DAYS_MOM,
        tp1_mult=_f.TP1_MULT,
        tp1_ratio=_f.TP1_RATIO,
        sector_penalty_threshold=_f.SECTOR_PENALTY_THRESHOLD,
        sector_penalty_pts=_f.SECTOR_PENALTY_PTS,
        sector_bonus_threshold=_f.SECTOR_BONUS_THRESHOLD,
        sector_bonus_pts=_f.SECTOR_BONUS_PTS,
    )

    t_total = time.time()
    best_stats = None
    prev_metric = -9999.0
    # 진동 감지용: (scores, params, stats) 이력
    history: list[tuple[dict, Params, Stats]] = []

    for iteration in range(1, max_iter + 1):
        log.info(f"\n{'=' * 70}")
        log.info(f"  교대 최적화 [{iteration}/{max_iter}]")
        log.info(f"{'=' * 70}")

        # ── 1단계: 신호 점수 최적화 (A: 고배점 → B: 저배점)
        score_min = (
            current_params.invest_min_score if iteration > 1
            else _SCORE_MIN_SCORE
        )
        scores_a = optimize_scores(
            cache, current_params, n_workers,
            min_score=score_min,
            current_scores=current_scores,
            score_grid=SCORE_GRID_A,
            label="A: 고배점",
        )
        new_scores = optimize_scores(
            cache, current_params, n_workers,
            min_score=score_min,
            current_scores=scores_a,
            score_grid=SCORE_GRID_B,
            label="B: 저배점",
        )

        # ── 캐시 score 재계산 + flat 배열 동기화
        recalc_cache_scores(cache, new_scores)
        cache.update_flat_scores()

        # ── 2단계: 매매파라미터 최적화 (min_score 포함)
        result = _run_param_grid(cache, n_workers, sector)
        if result is None:
            log.warning("  2단계 유효 결과 없음 → 중단")
            break
        new_params, best_stats = result
        history.append((new_scores, new_params, best_stats))

        # ── 수렴 체크: 완전 일치
        if new_scores == current_scores and new_params == current_params:
            log.info(f"\n  수렴 완료 (iteration {iteration})")
            current_scores = new_scores
            current_params = new_params
            break

        # ── metric 변화율 수렴: 개선폭 < 1% → 실질 수렴 (#4)
        cur_metric = best_stats.metric
        if prev_metric > 0 and cur_metric > 0:
            improvement = (cur_metric - prev_metric) / prev_metric
            if abs(improvement) < 0.01:
                log.info(
                    f"\n  metric 수렴 (변화 {improvement:+.2%},"
                    f" {prev_metric:.1f} → {cur_metric:.1f})"
                )
                current_scores = new_scores
                current_params = new_params
                break
        prev_metric = cur_metric

        # ── 2-cycle 진동 감지: 2회 전과 동일하면 더 나은 쪽 선택 후 종료
        if len(history) >= 3:
            prev2_scores, prev2_params, prev2_stats = history[-3]
            if new_scores == prev2_scores and new_params == prev2_params:
                prev1_scores, prev1_params, prev1_stats = history[-2]
                if best_stats.metric >= prev1_stats.metric:
                    log.info(f"\n  진동 감지 → 현재 iteration 선택 (metric {best_stats.metric:.1f})")
                    current_scores = new_scores
                    current_params = new_params
                else:
                    log.info(f"\n  진동 감지 → 이전 iteration 선택 (metric {prev1_stats.metric:.1f})")
                    current_scores = prev1_scores
                    current_params = prev1_params
                    best_stats = prev1_stats
                    recalc_cache_scores(cache, current_scores)
                break

        current_scores = new_scores
        current_params = new_params

    total_elapsed = time.time() - t_total
    log.info(f"\n교대 최적화 완료: {total_elapsed:.0f}초 ({total_elapsed / 60:.1f}분)")

    if apply and best_stats:
        _update_config_file(
            current_params, best_stats, start_date, end_date, total_elapsed,
            best_scores=current_scores,
        )
    elif not apply:
        log.info("\n  ⚠️  --no-apply: config.py 갱신 생략")


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
        "--no-sector", action="store_true", help="업종지수 패널티 비활성화"
    )
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
    args = parser.parse_args()

    if args.alternating:
        run_alternating(
            years=args.years,
            rebuild=args.rebuild,
            sector=not args.no_sector,
            apply=not args.no_apply,
            workers=args.workers,
            max_iter=args.max_iter,
        )
    else:
        run(
            years=args.years,
            rebuild=args.rebuild,
            sector=not args.no_sector,
            apply=not args.no_apply,
            workers=args.workers,
        )
