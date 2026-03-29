"""
신호 플래그 성능 분석 + ON/OFF 그리드 탐색
──────────────────────────────────────────
Step 1: 각 플래그별 승률/손실률/평균수익률 분석 (손실편향 플래그 탐지)
Step 2: 손실 편향 플래그를 ON/OFF 조합으로 백테스트 (제거 효과 검증)

실행:
    uv run python -m wye.blsh.domestic.optimize.signal_analysis
    uv run python -m wye.blsh.domestic.optimize.signal_analysis --top 5
    uv run python -m wye.blsh.domestic.optimize.signal_analysis --rebuild
    uv run python -m wye.blsh.domestic.optimize.signal_analysis --years 1
"""

import argparse
import logging
from dataclasses import dataclass
from itertools import product

from wye.blsh.common import dtutils
from wye.blsh.domestic.optimize._cache import (
    CACHE_DIR, OptCache, build_or_load,
    _SCORES, _REVERSAL_FLAGS, _MOMENTUM_FLAGS, _ALL_FLAGS,
)
from wye.blsh.domestic.optimize.grid_search import Params, Stats, _simulate_one

log = logging.getLogger(__name__)

# 수급 플래그 — 스캐너 신호가 아닌 외부 수급 데이터에서 추가되므로 분석 제외
_SUPPLY_FLAGS = frozenset({"F_TRN", "I_TRN", "F_C3", "I_C3", "F_1", "I_1", "FI", "P_OV"})


# ─────────────────────────────────────────
# 헬퍼
# ─────────────────────────────────────────
def _classify_mode(flags: set) -> str:
    rev_cnt = len(flags & _REVERSAL_FLAGS)
    mom_cnt = len(flags & _MOMENTUM_FLAGS)
    if mom_cnt >= 2 and mom_cnt > rev_cnt:
        return "MOM"
    if rev_cnt >= 2 and rev_cnt > mom_cnt:
        return "REV"
    if mom_cnt > 0 and rev_cnt > 0:
        return "MIX"
    return "WEAK"


def _calc_score(flags: set, mode: str) -> int:
    mom = sum(_SCORES[f] for f in flags & _MOMENTUM_FLAGS)
    rev = sum(_SCORES[f] for f in flags & _REVERSAL_FLAGS)
    neu = sum(_SCORES.get(f, 0) for f in flags - _ALL_FLAGS)
    if mode == "MOM":
        return mom + neu
    if mode == "REV":
        return rev + neu
    if mode == "MIX":
        return max(mom, rev) + neu
    return mom + rev + neu


# ─────────────────────────────────────────
# 플래그 통계 클래스
# ─────────────────────────────────────────
@dataclass
class FlagStats:
    flag: str
    count: int = 0      # 신호에 포함된 횟수
    trades: int = 0     # 실제 매수 성공 횟수
    wins: int = 0       # 수익 (익절 + 미확정수익)
    losses: int = 0     # 손실 (손절 + 미확정손실)
    total_ret: float = 0.0

    @property
    def win_rate(self) -> float:
        return self.wins / self.trades * 100 if self.trades else 0.0

    @property
    def loss_rate(self) -> float:
        return self.losses / self.trades * 100 if self.trades else 0.0

    @property
    def avg_ret(self) -> float:
        return self.total_ret / self.trades if self.trades else 0.0

    @property
    def loss_bias(self) -> float:
        """손실률 - 승률. 양수일수록 손실 편향."""
        return self.loss_rate - self.win_rate


# ─────────────────────────────────────────
# Step 1: 플래그별 성능 통계
# ─────────────────────────────────────────
def analyze_flags(cache: OptCache, params: Params) -> tuple[list[FlagStats], Stats]:
    """현재 파라미터로 전체 기간 시뮬레이션하며 플래그별 성과 집계.

    Returns:
        (flag_stats_sorted_by_loss_bias, baseline_stats)
    """
    flag_stats: dict[str, FlagStats] = {}
    baseline = Stats()

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
            is_win = result_type.startswith("익절") or ret_pct > 0
            is_loss = (result_type == "손절") or ret_pct < 0

            # 베이스라인 집계
            baseline.trades += 1
            baseline.total_ret += ret_pct
            if result_type.startswith("익절"):
                baseline.wins += 1
            elif result_type == "손절":
                baseline.losses += 1
            else:
                baseline.holds += 1

            # 플래그별 집계
            flags = set(sig["flags"].split(",")) if sig["flags"] else set()
            for f in flags:
                if f not in flag_stats:
                    flag_stats[f] = FlagStats(flag=f)
                fs = flag_stats[f]
                fs.count += 1
                fs.trades += 1
                fs.total_ret += ret_pct
                if is_win:
                    fs.wins += 1
                if is_loss:
                    fs.losses += 1

    return sorted(flag_stats.values(), key=lambda x: x.loss_bias, reverse=True), baseline


# ─────────────────────────────────────────
# Step 2: ON/OFF 그리드
# ─────────────────────────────────────────
def _backtest_disabled(cache: OptCache, params: Params, disabled: frozenset[str]) -> Stats:
    """지정 플래그를 비활성화하여 백테스트.

    disabled: 제거할 기본 신호 플래그 집합 (수급 플래그는 포함 불가)
    스코어는 캐시된 공급 보너스를 보존한 채 재계산.
    """
    st = Stats()

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

            # 수급 플래그 분리
            orig_signal_flags = orig_flags - _SUPPLY_FLAGS

            # 지정 플래그 제거
            new_signal_flags = orig_signal_flags - disabled
            if not new_signal_flags:
                continue

            new_mode = _classify_mode(new_signal_flags)
            if new_mode not in ("MOM", "MIX", "REV"):
                continue

            # 스코어: 새 신호 플래그 점수 + 원래 수급 보너스 보존
            new_base_score = _calc_score(new_signal_flags, new_mode)
            orig_base_score = _calc_score(
                orig_signal_flags, _classify_mode(orig_signal_flags) or new_mode
            )
            supply_bonus = sig["score"] - orig_base_score  # 캐시된 수급 기여분
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


def run_signal_grid(
    cache: OptCache, params: Params, suspicious: list[str]
) -> list[tuple[frozenset, Stats]]:
    """suspicious 플래그들의 ON/OFF 2^N 조합 탐색."""
    n = len(suspicious)
    results: list[tuple[frozenset, Stats]] = []

    for bits in product([False, True], repeat=n):  # False=ON, True=OFF(disable)
        disabled = frozenset(f for f, off in zip(suspicious, bits) if off)
        st = _backtest_disabled(cache, params, disabled)
        results.append((disabled, st))

    results.sort(key=lambda x: x[1].metric, reverse=True)
    return results


# ─────────────────────────────────────────
# 리포트
# ─────────────────────────────────────────
def _report_flags(flag_stats: list[FlagStats], baseline: Stats, min_trades: int = 30):
    log.info("")
    log.info("=" * 105)
    log.info("  Step 1: 플래그별 손익 분석  (손실편향 내림차순)")
    log.info("=" * 105)
    log.info(
        f"  {'플래그':<10s}  {'출현':>6s}  {'거래':>6s}  "
        f"{'승률':>6s}  {'손실률':>6s}  {'평균수익':>8s}  {'손실편향':>8s}  비고"
    )
    log.info("-" * 105)

    base_flags = [fs for fs in flag_stats if fs.flag not in _SUPPLY_FLAGS and fs.trades >= min_trades]
    supply_flags_list = [fs for fs in flag_stats if fs.flag in _SUPPLY_FLAGS and fs.trades >= min_trades]

    def _print_row(fs: FlagStats, label: str = ""):
        marker = ""
        if fs.loss_bias > 15:
            marker = "◀ 손실편향"
        elif fs.loss_bias < -10:
            marker = "▶ 우수"
        log.info(
            f"  {fs.flag:<10s}  {fs.count:>5d}건  {fs.trades:>5d}건  "
            f"{fs.win_rate:>5.1f}%  {fs.loss_rate:>5.1f}%  "
            f"{fs.avg_ret:>+7.2f}%  {fs.loss_bias:>+7.1f}%  {marker}"
        )

    log.info("  [ 기본 신호 플래그 ]")
    for fs in base_flags:
        _print_row(fs)

    if supply_flags_list:
        log.info("  [ 수급 플래그 ]")
        for fs in supply_flags_list:
            _print_row(fs)

    log.info("-" * 105)
    log.info(
        f"  전체 베이스라인: {baseline.trades}건  "
        f"승률 {baseline.win_rate:.1f}%  평균 {baseline.avg_ret:+.2f}%  총 {baseline.total_ret:+.1f}%"
    )
    log.info("=" * 105)


def _report_grid(
    suspicious: list[str],
    results: list[tuple[frozenset, Stats]],
    baseline: Stats,
):
    log.info("")
    log.info("=" * 115)
    log.info(f"  Step 2: ON/OFF 그리드  의심 플래그: {suspicious}  ({2**len(suspicious)}조합)")
    log.info("=" * 115)
    log.info(
        f"  {'#':>3s}  {'거래':>6s}  {'승률':>6s}  {'손절률':>6s}  "
        f"{'평균수익':>8s}  {'총수익':>10s}  {'metric':>8s}  │ OFF 플래그  (Δ총수익)"
    )
    log.info("-" * 115)

    # 베이스라인 먼저 출력
    bl_loss_rate = baseline.losses / baseline.trades * 100 if baseline.trades else 0
    log.info(
        f"  {'BASE':>3s}  {baseline.trades:>5d}건  {baseline.win_rate:>5.1f}%  "
        f"{bl_loss_rate:>5.1f}%  "
        f"{baseline.avg_ret:>+7.2f}%  {baseline.total_ret:>+9.1f}%  "
        f"{baseline.metric:>8.1f}  │ (모두 ON)"
    )
    log.info("-" * 115)

    for rank, (disabled, st) in enumerate(results[:20], 1):
        disabled_str = ", ".join(sorted(disabled)) if disabled else "(모두 ON)"
        delta_ret = st.total_ret - baseline.total_ret
        loss_rate = st.losses / st.trades * 100 if st.trades else 0
        log.info(
            f"  {rank:3d}  {st.trades:>5d}건  {st.win_rate:>5.1f}%  "
            f"{loss_rate:>5.1f}%  "
            f"{st.avg_ret:>+7.2f}%  {st.total_ret:>+9.1f}%  "
            f"{st.metric:>8.1f}  │ {disabled_str}  ({delta_ret:+.1f}%)"
        )

    log.info("=" * 115)

    if results:
        best_disabled, best_st = results[0]
        if best_st.metric > baseline.metric:
            delta = best_st.total_ret - baseline.total_ret
            log.info(f"\n  ★ 권장: {sorted(best_disabled) if best_disabled else '변경 없음'} 비활성화")
            log.info(
                f"    총수익 {baseline.total_ret:+.1f}% → {best_st.total_ret:+.1f}% "
                f"(Δ{delta:+.1f}%)  거래 {best_st.trades}건"
            )
        else:
            log.info("\n  → 현재 플래그 설정이 최적 (제거 효과 없음)")


# ─────────────────────────────────────────
# 메인
# ─────────────────────────────────────────
def run(years: int = 2, rebuild: bool = False, top: int = 5):
    log.info(f"분석 기간: {years}년")

    end_date = dtutils.today()
    start_date = dtutils.add_days(end_date, -years * 365)
    log.info(f"분석 기간: {start_date} ~ {end_date}")

    if rebuild:
        for p in CACHE_DIR.glob("opt_cache*.pkl"):
            p.unlink()
            log.info(f"캐시 삭제: {p}")

    cache = build_or_load(start_date, end_date)

    # 현재 factor 상수 → Params
    from wye.blsh.domestic import factor as _f
    params = Params(
        invest_min_score=_f.INVEST_MIN_SCORE,
        atr_sl_mult=_f.ATR_SL_MULT,
        atr_tp_mult=_f.ATR_TP_MULT,
        max_hold_days_rev=_f.MAX_HOLD_DAYS,
        max_hold_days_mix=_f.MAX_HOLD_DAYS_MIX,
        max_hold_days_mom=_f.MAX_HOLD_DAYS_MOM,
        tp1_mult=_f.TP1_MULT,
        tp1_ratio=_f.TP1_RATIO,
        gap_down_limit=_f.GAP_DOWN_LIMIT,
        sector_penalty_threshold=_f.SECTOR_PENALTY_THRESHOLD,
        sector_penalty_pts=_f.SECTOR_PENALTY_PTS,
        sector_bonus_pts=_f.SECTOR_BONUS_PTS,
    )
    log.info(f"파라미터: {params.label()}")

    # ── Step 1
    log.info("\n[Step 1] 플래그별 성능 분석 중...")
    flag_stats, baseline = analyze_flags(cache, params)
    _report_flags(flag_stats, baseline)

    # 의심 플래그 선정: 거래 30건 이상, loss_bias > 0, 기본 신호 플래그만
    suspicious = [
        fs.flag
        for fs in flag_stats
        if fs.trades >= 30
        and fs.loss_bias > 0
        and fs.flag not in _SUPPLY_FLAGS
    ][:top]

    if not suspicious:
        log.info("\n  의심 플래그 없음 — Step 2 생략")
        return

    # ── Step 2
    n_combos = 2 ** len(suspicious)
    log.info(f"\n[Step 2] ON/OFF 그리드  ({len(suspicious)}개 플래그, {n_combos}조합)...")
    grid_results = run_signal_grid(cache, params, suspicious)
    _report_grid(suspicious, grid_results, baseline)


# ─────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="신호 플래그 손실 편향 분석")
    parser.add_argument("--years", type=int, default=2, help="분석 기간 (년, 기본 2)")
    parser.add_argument("--rebuild", action="store_true", help="캐시 강제 재빌드")
    parser.add_argument("--top", type=int, default=5,
                        help="Step 2에 사용할 의심 플래그 수 (기본 5)")
    args = parser.parse_args()

    run(years=args.years, rebuild=args.rebuild, top=args.top)
