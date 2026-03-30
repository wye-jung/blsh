"""
ROV 플래그 처리 방식 비교 분석
────────────────────────────────
ROV = "RSI 30 미만 지속 중" (과매도 상태, 반등 미확인)

현재: _REVERSAL_FLAGS에 포함 → rev_cnt에 기여, REV/MIX mode 진입 가능
제안: _NEUTRAL 처리 → rev_cnt 불포함, 점수만 neu_score에 기여

세 가지 시나리오 비교:
  A. 현재 (ROV in _REVERSAL_FLAGS)
  B. ROV → neutral (rev_cnt 미기여, 점수 +1 유지)
  C. ROV 완전 제거 (rev_cnt 미기여, 점수도 0)

실행:
    uv run python -m wye.blsh.domestic.optimize.rov_test
"""

import logging
from wye.blsh.common import dtutils
from wye.blsh.domestic.optimize._cache import (
    build_or_load, OptCache,
    _SCORES, _REVERSAL_FLAGS, _MOMENTUM_FLAGS, _ALL_FLAGS,
)
from wye.blsh.domestic.optimize.grid_search import Params, Stats, _simulate_one

log = logging.getLogger(__name__)

_SUPPLY_FLAGS = frozenset({"F_TRN", "I_TRN", "F_C3", "I_C3", "F_1", "I_1", "FI", "P_OV"})


def _classify_mode(rev_flags: frozenset, mom_flags: frozenset, flags: set) -> str:
    rev_cnt = len(flags & rev_flags)
    mom_cnt = len(flags & mom_flags)
    if mom_cnt >= 2 and mom_cnt > rev_cnt:
        return "MOM"
    if rev_cnt >= 2 and rev_cnt > mom_cnt:
        return "REV"
    if mom_cnt > 0 and rev_cnt > 0:
        return "MIX"
    return "WEAK"


def _calc_score(rev_flags: frozenset, mom_flags: frozenset, all_flags: frozenset,
                flags: set, mode: str) -> int:
    mom = sum(_SCORES.get(f, 0) for f in flags & mom_flags)
    rev = sum(_SCORES.get(f, 0) for f in flags & rev_flags)
    neu = sum(_SCORES.get(f, 0) for f in flags - all_flags)
    if mode == "MOM":
        return mom + neu
    if mode == "REV":
        return rev + neu
    if mode == "MIX":
        return max(mom, rev) + neu
    return mom + rev + neu


def _simulate_scenario(
    cache: OptCache, params: Params,
    rev_flags: frozenset, mom_flags: frozenset, all_flags: frozenset,
    rov_score: int,  # ROV의 점수 기여 (0=제거, 1=유지)
) -> Stats:
    st = Stats()
    orig_rev_flags = _REVERSAL_FLAGS
    orig_mom_flags = _MOMENTUM_FLAGS
    orig_all_flags = _ALL_FLAGS

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

            signal_flags = orig_flags - _SUPPLY_FLAGS

            new_mode = _classify_mode(rev_flags, mom_flags, signal_flags)
            if new_mode not in ("MOM", "MIX", "REV"):
                continue

            # 새 분류 기준으로 기술 점수 재계산
            new_base_score = _calc_score(rev_flags, mom_flags, all_flags, signal_flags, new_mode)
            # ROV가 있고 neutral 처리 시 점수 재조정
            if "ROV" in signal_flags and rov_score != _SCORES.get("ROV", 1):
                delta = rov_score - _SCORES.get("ROV", 1)
                new_base_score += delta

            # 원래 수급 기여분 보존
            orig_base_score = _calc_score(orig_rev_flags, orig_mom_flags, orig_all_flags,
                                          signal_flags, _classify_mode(orig_rev_flags, orig_mom_flags, signal_flags) or new_mode)
            supply_bonus = sig["score"] - orig_base_score
            new_score = new_base_score + supply_bonus

            sec_gap = sig.get("sector_gap", 0.0)
            if params.sector_penalty_pts != 0 and sec_gap < params.sector_penalty_threshold:
                new_score += params.sector_penalty_pts
            elif params.sector_bonus_pts != 0 and sec_gap >= 0:
                new_score += params.sector_bonus_pts

            if new_score < params.invest_min_score:
                continue

            sim_sig = {**sig, "mode": new_mode}
            res = _simulate_one(sim_sig, entry_date, cache.ohlcv_idx, params, hold_dates)
            if res is None:
                continue

            result_type, ret_pct = res
            st.trades += 1
            st.total_ret += ret_pct
            if result_type.startswith("익절"):
                st.wins += 1
            elif result_type == "손절":
                st.losses += 1
            else:
                st.holds += 1

    return st


def _rov_combo_stats(cache: OptCache, params: Params) -> None:
    """ROV 조합별 mode 분포와 성과 분석."""
    from collections import defaultdict

    # 각 ROV 포함 신호의 mode와 다른 플래그 조합 집계
    combo_data: dict[str, dict] = defaultdict(lambda: {"count": 0, "trades": 0,
                                                        "wins": 0, "losses": 0, "ret": 0.0})

    orig_rev_flags = _REVERSAL_FLAGS
    orig_mom_flags = _MOMENTUM_FLAGS

    for base_date in cache.scan_dates:
        entry_date = cache.next_biz.get(base_date)
        if not entry_date:
            continue
        sigs = cache.signals.get(base_date, [])
        hold_dates = cache.forward_dates.get(entry_date, [entry_date])

        for sig in sigs:
            orig_flags = set(sig["flags"].split(",")) if sig["flags"] else set()
            if "P_OV" in orig_flags or "ROV" not in orig_flags:
                continue
            if sig["mode"] not in ("MOM", "MIX", "REV"):
                continue

            signal_flags = orig_flags - _SUPPLY_FLAGS
            paired_rev = sorted((signal_flags & orig_rev_flags) - {"ROV"})
            paired_mom = sorted(signal_flags & orig_mom_flags)
            mode = sig["mode"]
            key = f"mode={mode} rev={paired_rev or '-'} mom={paired_mom or '-'}"

            combo_data[key]["count"] += 1

            effective_score = sig["score"]
            sec_gap = sig.get("sector_gap", 0.0)
            if params.sector_penalty_pts != 0 and sec_gap < params.sector_penalty_threshold:
                effective_score += params.sector_penalty_pts
            elif params.sector_bonus_pts != 0 and sec_gap >= 0:
                effective_score += params.sector_bonus_pts
            if effective_score < params.invest_min_score:
                continue

            res = _simulate_one(sig, entry_date, cache.ohlcv_idx, params, hold_dates)
            if res is None:
                continue
            result_type, ret_pct = res
            combo_data[key]["trades"] += 1
            combo_data[key]["ret"] += ret_pct
            if result_type.startswith("익절"):
                combo_data[key]["wins"] += 1
            elif result_type == "손절":
                combo_data[key]["losses"] += 1

    log.info("")
    log.info("=" * 110)
    log.info("  ROV 조합별 mode 분포 및 성과 (거래 10건 이상)")
    log.info("=" * 110)
    log.info(f"  {'조합':<55s}  {'신호':>5s}  {'거래':>5s}  {'승률':>6s}  {'손절률':>6s}  {'평균수익':>8s}")
    log.info("-" * 110)
    for key, d in sorted(combo_data.items(), key=lambda x: -x[1]["trades"]):
        if d["trades"] < 10:
            continue
        wr = d["wins"] / d["trades"] * 100 if d["trades"] else 0
        lr = d["losses"] / d["trades"] * 100 if d["trades"] else 0
        ar = d["ret"] / d["trades"] if d["trades"] else 0
        log.info(f"  {key:<55s}  {d['count']:>4d}건  {d['trades']:>4d}건  "
                 f"{wr:>5.1f}%  {lr:>5.1f}%  {ar:>+7.2f}%")
    log.info("=" * 110)


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
        sector_penalty_threshold=_f.SECTOR_PENALTY_THRESHOLD,
        sector_penalty_pts=_f.SECTOR_PENALTY_PTS,
        sector_bonus_pts=_f.SECTOR_BONUS_PTS,
    )

    # ROV 조합별 상세 분석
    _rov_combo_stats(cache, params)

    # 세 가지 시나리오 비교
    # A. 현재 (ROV in _REVERSAL_FLAGS, score=1)
    rev_A = _REVERSAL_FLAGS                            # ROV 포함
    all_A = _ALL_FLAGS

    # B. ROV → neutral (rev_cnt 미기여, score=1 유지)
    rev_B = _REVERSAL_FLAGS - {"ROV"}
    all_B = _ALL_FLAGS - {"ROV"}                       # neu_score로 계산되도록 ALL_FLAGS에서 제거

    # C. ROV 완전 제거 (rev_cnt 미기여, score=0)
    rev_C = _REVERSAL_FLAGS - {"ROV"}
    all_C = _ALL_FLAGS                                  # ALL_FLAGS에 남겨 neu 계산도 안 되도록

    scenarios = [
        ("A: 현재 (ROV = REVERSAL +1)", rev_A, _MOMENTUM_FLAGS, all_A, 1),
        ("B: ROV → neutral (+1 유지)",  rev_B, _MOMENTUM_FLAGS, all_B, 1),
        ("C: ROV 완전 제거 (점수 0)",   rev_C, _MOMENTUM_FLAGS, all_C, 0),
    ]

    results = []
    for label, rev_f, mom_f, all_f, rov_sc in scenarios:
        log.info(f"  → {label} 시뮬레이션 중...")
        st = _simulate_scenario(cache, params, rev_f, mom_f, all_f, rov_sc)
        results.append((label, st))

    log.info("")
    log.info("=" * 100)
    log.info("  ROV 처리 방식 비교")
    log.info("=" * 100)
    log.info(f"  {'시나리오':<35s}  {'거래':>6s}  {'승률':>6s}  {'손절률':>6s}  "
             f"{'평균수익':>8s}  {'총수익':>10s}  {'metric':>8s}")
    log.info("-" * 100)
    for label, st in results:
        loss_rate = st.losses / st.trades * 100 if st.trades else 0
        log.info(f"  {label:<35s}  {st.trades:>5d}건  {st.win_rate:>5.1f}%  "
                 f"{loss_rate:>5.1f}%  {st.avg_ret:>+7.2f}%  "
                 f"{st.total_ret:>+9.1f}%  {st.metric:>8.1f}")
    log.info("=" * 100)


if __name__ == "__main__":
    import argparse
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )
    parser = argparse.ArgumentParser(description="ROV 플래그 처리 방식 비교")
    parser.add_argument("--years", type=int, default=2)
    args = parser.parse_args()
    run(years=args.years)
