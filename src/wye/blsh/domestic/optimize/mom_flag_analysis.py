"""
모멘텀 플래그 (MAA·MGC·PB·VS) 심층 분석
────────────────────────────────────────
signal_analysis Step 2에서 이 4개 플래그 비활성화 시 성과가 크게 개선됨 → 원인 분석

분석 항목:
  [1] mode별 성과 (MOM vs REV vs MIX) — 모멘텀 모드 자체가 문제인가?
  [2] score 구간별 성과 (9, 10, 11, 12+) — 경계선 신호(score=9)가 주요 원인인가?
  [3] 4개 플래그의 "진입 인에이블러" 분석
       — 해당 플래그 없이 score<9이거나 mode=WEAK → 이 플래그가 투자 진입을 가능케 한 경우
  [4] 플래그 단독 효과 (각 1개씩 비활성화 시 개선폭)

실행:
    uv run python -m wye.blsh.domestic.optimize.mom_flag_analysis
    uv run python -m wye.blsh.domestic.optimize.mom_flag_analysis --years 2
"""

import argparse
import logging
from collections import defaultdict

from wye.blsh.common import dtutils
from wye.blsh.domestic.optimize._cache import (
    build_or_load, OptCache,
    _SCORES, _REVERSAL_FLAGS, _MOMENTUM_FLAGS, _ALL_FLAGS,
)
from wye.blsh.domestic.optimize.grid_search import Params, Stats, _simulate_one
from wye.blsh.domestic.optimize.signal_analysis import (
    _classify_mode, _calc_score, _backtest_disabled, _SUPPLY_FLAGS,
)

log = logging.getLogger(__name__)

_TARGET_FLAGS = ["MAA", "MGC", "PB", "VS"]


# ─────────────────────────────────────────
# [1] mode별 성과
# ─────────────────────────────────────────
def analyze_by_mode(cache: OptCache, params: Params) -> None:
    mode_stats: dict[str, Stats] = {m: Stats() for m in ("MOM", "REV", "MIX")}

    for base_date in cache.scan_dates:
        entry_date = cache.next_biz.get(base_date)
        if not entry_date:
            continue
        sigs = cache.signals.get(base_date, [])
        hold_dates = cache.forward_dates.get(entry_date, [entry_date])

        for sig in sigs:
            if "P_OV" in sig["flags"]:
                continue
            mode = sig["mode"]
            if mode not in mode_stats:
                continue

            effective_score = sig["score"]
            if effective_score < params.invest_min_score:
                continue

            res = _simulate_one(sig, entry_date, cache.ohlcv_idx, params, hold_dates)
            if res is None:
                continue

            result_type, ret_pct = res
            st = mode_stats[mode]
            st.trades += 1
            st.total_ret += ret_pct
            if result_type.startswith("익절"):
                st.wins += 1
            elif result_type == "손절":
                st.losses += 1
            else:
                st.holds += 1

    log.info("")
    log.info("=" * 90)
    log.info("  [1] mode별 성과")
    log.info("=" * 90)
    log.info(f"  {'mode':<8s}  {'거래':>6s}  {'승률':>6s}  {'손절률':>6s}  {'평균수익':>8s}  {'총수익':>10s}  {'metric':>8s}")
    log.info("-" * 90)
    total = Stats()
    for mode in ("MOM", "REV", "MIX"):
        st = mode_stats[mode]
        if st.trades == 0:
            continue
        loss_rate = st.losses / st.trades * 100
        log.info(f"  {mode:<8s}  {st.trades:>5d}건  {st.win_rate:>5.1f}%  "
                 f"{loss_rate:>5.1f}%  {st.avg_ret:>+7.2f}%  "
                 f"{st.total_ret:>+9.1f}%  {st.metric:>8.1f}")
        total.trades += st.trades
        total.wins += st.wins
        total.losses += st.losses
        total.holds += st.holds
        total.total_ret += st.total_ret
    log.info("-" * 90)
    loss_rate = total.losses / total.trades * 100 if total.trades else 0
    log.info(f"  {'합계':<8s}  {total.trades:>5d}건  {total.win_rate:>5.1f}%  "
             f"{loss_rate:>5.1f}%  {total.avg_ret:>+7.2f}%  "
             f"{total.total_ret:>+9.1f}%  {total.metric:>8.1f}")
    log.info("=" * 90)


# ─────────────────────────────────────────
# [2] score 구간별 성과
# ─────────────────────────────────────────
def analyze_by_score(cache: OptCache, params: Params) -> None:
    buckets = defaultdict(Stats)

    for base_date in cache.scan_dates:
        entry_date = cache.next_biz.get(base_date)
        if not entry_date:
            continue
        sigs = cache.signals.get(base_date, [])
        hold_dates = cache.forward_dates.get(entry_date, [entry_date])

        for sig in sigs:
            if "P_OV" in sig["flags"]:
                continue
            if sig["mode"] not in ("MOM", "MIX", "REV"):
                continue

            effective_score = sig["score"]
            if effective_score < params.invest_min_score:
                continue

            res = _simulate_one(sig, entry_date, cache.ohlcv_idx, params, hold_dates)
            if res is None:
                continue

            result_type, ret_pct = res
            bucket = effective_score if effective_score <= 12 else 13
            st = buckets[bucket]
            st.trades += 1
            st.total_ret += ret_pct
            if result_type.startswith("익절"):
                st.wins += 1
            elif result_type == "손절":
                st.losses += 1
            else:
                st.holds += 1

    log.info("")
    log.info("=" * 90)
    log.info("  [2] score 구간별 성과")
    log.info("=" * 90)
    log.info(f"  {'score':<8s}  {'거래':>6s}  {'승률':>6s}  {'손절률':>6s}  {'평균수익':>8s}  {'총수익':>10s}  {'metric':>8s}")
    log.info("-" * 90)
    for score in sorted(buckets.keys()):
        st = buckets[score]
        label = f"{score}" if score <= 12 else "13+"
        loss_rate = st.losses / st.trades * 100 if st.trades else 0
        log.info(f"  {label:<8s}  {st.trades:>5d}건  {st.win_rate:>5.1f}%  "
                 f"{loss_rate:>5.1f}%  {st.avg_ret:>+7.2f}%  "
                 f"{st.total_ret:>+9.1f}%  {st.metric:>8.1f}")
    log.info("=" * 90)


# ─────────────────────────────────────────
# [3] 인에이블러 분석 — 특정 플래그가 없으면 진입 불가했을 신호
# ─────────────────────────────────────────
def analyze_enabler(cache: OptCache, params: Params) -> None:
    """각 플래그가 없으면 score<min 또는 mode=WEAK → '이 플래그 덕에 진입' 신호 성과."""

    enabler_stats: dict[str, dict] = {
        f: {"enabled": Stats(), "not_enabled": Stats()} for f in _TARGET_FLAGS
    }

    for base_date in cache.scan_dates:
        entry_date = cache.next_biz.get(base_date)
        if not entry_date:
            continue
        sigs = cache.signals.get(base_date, [])
        hold_dates = cache.forward_dates.get(entry_date, [entry_date])

        for sig in sigs:
            orig_flags = set(sig["flags"].split(",")) if sig["flags"] else set()
            if "P_OV" in orig_flags:
                continue
            if sig["mode"] not in ("MOM", "MIX", "REV"):
                continue

            effective_score = sig["score"]
            if effective_score < params.invest_min_score:
                continue

            res = _simulate_one(sig, entry_date, cache.ohlcv_idx, params, hold_dates)
            if res is None:
                continue
            result_type, ret_pct = res

            signal_flags = orig_flags - _SUPPLY_FLAGS
            supply_bonus = sig["score"] - _calc_score(
                signal_flags, _classify_mode(signal_flags)
            )

            for flag in _TARGET_FLAGS:
                if flag not in orig_flags:
                    continue
                # 이 플래그 없이 점수/모드 재계산
                reduced = signal_flags - {flag}
                new_mode = _classify_mode(reduced)
                if new_mode not in ("MOM", "MIX", "REV"):
                    is_enabled = True
                else:
                    new_score = _calc_score(reduced, new_mode) + supply_bonus
                    is_enabled = new_score < params.invest_min_score

                key = "enabled" if is_enabled else "not_enabled"
                st = enabler_stats[flag][key]
                st.trades += 1
                st.total_ret += ret_pct
                if result_type.startswith("익절"):
                    st.wins += 1
                elif result_type == "손절":
                    st.losses += 1
                else:
                    st.holds += 1

    log.info("")
    log.info("=" * 110)
    log.info("  [3] 플래그 인에이블러 분석  (해당 플래그 없으면 진입 불가 vs 진입 가능)")
    log.info("=" * 110)
    log.info(f"  {'플래그':<6s}  {'구분':<14s}  {'거래':>6s}  {'승률':>6s}  {'손절률':>6s}  {'평균수익':>8s}  {'총수익':>10s}")
    log.info("-" * 110)
    for flag in _TARGET_FLAGS:
        for key, label in [("enabled", "진입가능케함"), ("not_enabled", "다른신호로도가능")]:
            st = enabler_stats[flag][key]
            if st.trades == 0:
                continue
            loss_rate = st.losses / st.trades * 100
            log.info(f"  {flag:<6s}  {label:<14s}  {st.trades:>5d}건  {st.win_rate:>5.1f}%  "
                     f"{loss_rate:>5.1f}%  {st.avg_ret:>+7.2f}%  {st.total_ret:>+9.1f}%")
        log.info("-" * 110)
    log.info("=" * 110)


# ─────────────────────────────────────────
# [4] 플래그 단독 비활성화 효과
# ─────────────────────────────────────────
def analyze_single_disable(cache: OptCache, params: Params, baseline: Stats) -> None:
    log.info("")
    log.info("=" * 90)
    log.info("  [4] 플래그 단독 비활성화 효과")
    log.info("=" * 90)
    log.info(f"  {'OFF 플래그':<10s}  {'거래':>6s}  {'승률':>6s}  {'손절률':>6s}  "
             f"{'평균수익':>8s}  {'총수익':>10s}  {'Δ총수익':>9s}  {'metric':>8s}")
    log.info("-" * 90)

    bl_loss = baseline.losses / baseline.trades * 100 if baseline.trades else 0
    log.info(f"  {'(베이스라인)':<10s}  {baseline.trades:>5d}건  {baseline.win_rate:>5.1f}%  "
             f"{bl_loss:>5.1f}%  {baseline.avg_ret:>+7.2f}%  "
             f"{baseline.total_ret:>+9.1f}%  {'':>9s}  {baseline.metric:>8.1f}")
    log.info("-" * 90)

    rows = []
    for flag in _TARGET_FLAGS:
        st = _backtest_disabled(cache, params, frozenset([flag]))
        rows.append((flag, st))
    # 전체 4개 동시 비활성화
    st_all = _backtest_disabled(cache, params, frozenset(_TARGET_FLAGS))
    rows.append(("모두 OFF", st_all))

    for label, st in rows:
        loss_rate = st.losses / st.trades * 100 if st.trades else 0
        delta = st.total_ret - baseline.total_ret
        log.info(f"  {label:<10s}  {st.trades:>5d}건  {st.win_rate:>5.1f}%  "
                 f"{loss_rate:>5.1f}%  {st.avg_ret:>+7.2f}%  "
                 f"{st.total_ret:>+9.1f}%  {delta:>+8.1f}%  {st.metric:>8.1f}")
    log.info("=" * 90)


# ─────────────────────────────────────────
def run(years: int = 2):
    end_date = dtutils.today()
    start_date = dtutils.add_days(end_date, -years * 365)
    log.info(f"기간: {start_date} ~ {end_date} ({years}년)")

    cache = build_or_load(start_date, end_date)

    from wye.blsh.domestic import config as _f
    params = Params(
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
    log.info(f"파라미터: {params.label()}")

    # 베이스라인 (signal_analysis와 동일)
    from wye.blsh.domestic.optimize.signal_analysis import analyze_flags
    _, baseline = analyze_flags(cache, params)

    analyze_by_mode(cache, params)
    analyze_by_score(cache, params)
    analyze_enabler(cache, params)
    analyze_single_disable(cache, params, baseline)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )
    parser = argparse.ArgumentParser(description="모멘텀 플래그 심층 분석")
    parser.add_argument("--years", type=int, default=2)
    args = parser.parse_args()
    run(years=args.years)
