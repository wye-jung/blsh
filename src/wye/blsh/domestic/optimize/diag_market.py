"""
시장별(KOSPI/KOSDAQ) 백테스트 성과 비교 진단
──────────────────────────────────────────
현재 config 파라미터로 전체 기간 시뮬레이션 후,
KOSPI/KOSDAQ별 trades, win_rate, avg_ret 분리 집계.

시장별 최적화 분리가 필요한지 데이터 기반으로 판단하기 위한 진단 도구.

실행:
    uv run python -m wye.blsh.domestic.optimize.diag_market
    uv run python -m wye.blsh.domestic.optimize.diag_market --years 1
"""

import argparse
import logging
import math
from collections import defaultdict
from dataclasses import dataclass, field

from wye.blsh.common import dtutils
from wye.blsh.domestic.optimize._cache import build_or_load, OptCache
from wye.blsh.domestic.optimize import grid_search as _gs
from wye.blsh.domestic.optimize.grid_search import Params, _simulate_one

log = logging.getLogger(__name__)


@dataclass
class MarketStats:
    trades: int = 0
    wins: int = 0
    losses: int = 0
    holds: int = 0
    total_ret: float = 0.0
    ret_sq: float = 0.0
    rets: list = field(default_factory=list)

    @property
    def win_rate(self) -> float:
        return 100 * self.wins / self.trades if self.trades else 0

    @property
    def loss_rate(self) -> float:
        return 100 * self.losses / self.trades if self.trades else 0

    @property
    def avg_ret(self) -> float:
        return self.total_ret / self.trades if self.trades else 0

    @property
    def std_ret(self) -> float:
        if self.trades < 2:
            return 0
        mean = self.avg_ret
        var = self.ret_sq / self.trades - mean * mean
        return math.sqrt(max(var, 0))

    @property
    def metric(self) -> float:
        if self.trades < 30:
            return -9999
        return self.avg_ret * math.sqrt(self.trades)


def analyze_by_market(cache: OptCache, params: Params) -> dict[str, MarketStats]:
    stats: dict[str, MarketStats] = defaultdict(MarketStats)

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
            elif params.sector_bonus_pts != 0 and sec_gap >= params.sector_bonus_threshold:
                effective_score += params.sector_bonus_pts
            if effective_score < params.invest_min_score:
                continue

            res = _simulate_one(sig, entry_date, cache.ohlcv_idx, params, hold_dates)
            if res is None:
                continue

            result_type, ret_pct = res
            market = sig["market"]
            ms = stats[market]
            ms.trades += 1
            ms.total_ret += ret_pct
            ms.ret_sq += ret_pct * ret_pct
            ms.rets.append(ret_pct)

            if result_type.startswith("익절"):
                ms.wins += 1
            elif result_type == "손절":
                ms.losses += 1
            else:
                ms.holds += 1

    return dict(stats)


def analyze_by_market_mode(cache: OptCache, params: Params) -> dict[tuple[str, str], MarketStats]:
    """시장 × mode(MOM/MIX/REV) 교차 분석."""
    stats: dict[tuple[str, str], MarketStats] = defaultdict(MarketStats)

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
            elif params.sector_bonus_pts != 0 and sec_gap >= params.sector_bonus_threshold:
                effective_score += params.sector_bonus_pts
            if effective_score < params.invest_min_score:
                continue

            res = _simulate_one(sig, entry_date, cache.ohlcv_idx, params, hold_dates)
            if res is None:
                continue

            result_type, ret_pct = res
            key = (sig["market"], sig["mode"])
            ms = stats[key]
            ms.trades += 1
            ms.total_ret += ret_pct
            ms.ret_sq += ret_pct * ret_pct
            ms.rets.append(ret_pct)

            if result_type.startswith("익절"):
                ms.wins += 1
            elif result_type == "손절":
                ms.losses += 1
            else:
                ms.holds += 1

    return dict(stats)


def _report(market_stats: dict[str, MarketStats]):
    print("\n" + "=" * 70)
    print("시장별 백테스트 성과 비교")
    print("=" * 70)

    total = MarketStats()
    for market in sorted(market_stats.keys()):
        ms = market_stats[market]
        total.trades += ms.trades
        total.wins += ms.wins
        total.losses += ms.losses
        total.holds += ms.holds
        total.total_ret += ms.total_ret
        total.ret_sq += ms.ret_sq

    print(f"\n{'시장':<8} {'거래':>6} {'승률':>8} {'손실률':>8} {'평균수익':>10} "
          f"{'표준편차':>10} {'총수익':>10} {'metric':>10}")
    print("-" * 70)

    for market in sorted(market_stats.keys()):
        ms = market_stats[market]
        print(f"{market:<8} {ms.trades:>6} {ms.win_rate:>7.1f}% {ms.loss_rate:>7.1f}% "
              f"{ms.avg_ret:>+9.2f}% {ms.std_ret:>9.2f}% "
              f"{ms.total_ret:>+9.1f}% {ms.metric:>10.1f}")

    print("-" * 70)
    print(f"{'전체':<8} {total.trades:>6} {total.win_rate:>7.1f}% {total.loss_rate:>7.1f}% "
          f"{total.avg_ret:>+9.2f}% {total.std_ret:>9.2f}% "
          f"{total.total_ret:>+9.1f}%")

    # 차이 분석
    markets = sorted(market_stats.keys())
    if len(markets) >= 2:
        m1, m2 = markets[0], markets[1]
        s1, s2 = market_stats[m1], market_stats[m2]
        print(f"\n── 차이 분석 ({m1} vs {m2}) ──")
        print(f"  승률 차이:     {s1.win_rate - s2.win_rate:+.1f}%p")
        print(f"  평균수익 차이: {s1.avg_ret - s2.avg_ret:+.2f}%p")
        print(f"  총수익 차이:   {s1.total_ret - s2.total_ret:+.1f}%p")

        diff = abs(s1.avg_ret - s2.avg_ret)
        if diff > 1.0:
            print(f"  → 평균수익 차이 {diff:.2f}%p: 시장별 분리 최적화 가치 있음 ⚠️")
        elif diff > 0.5:
            print(f"  → 평균수익 차이 {diff:.2f}%p: 경계선, 추가 분석 필요")
        else:
            print(f"  → 평균수익 차이 {diff:.2f}%p: 미미함, 분리 불필요 ✅")


def _report_market_mode(stats: dict[tuple[str, str], MarketStats]):
    print("\n" + "=" * 70)
    print("시장 × 모드 교차 분석")
    print("=" * 70)

    print(f"\n{'시장':<8} {'모드':<5} {'거래':>6} {'승률':>8} {'평균수익':>10} {'총수익':>10}")
    print("-" * 50)

    for (market, mode) in sorted(stats.keys()):
        ms = stats[(market, mode)]
        print(f"{market:<8} {mode:<5} {ms.trades:>6} {ms.win_rate:>7.1f}% "
              f"{ms.avg_ret:>+9.2f}% {ms.total_ret:>+9.1f}%")


def _report_score_distribution(cache: OptCache, params: Params, market_stats: dict[str, MarketStats]):
    """시장별 점수 분포."""
    print("\n" + "=" * 70)
    print("시장별 점수 분포 (최종 후보 기준)")
    print("=" * 70)

    score_dist: dict[str, dict[int, int]] = defaultdict(lambda: defaultdict(int))

    for base_date in cache.scan_dates:
        sigs = cache.signals.get(base_date, [])
        for sig in sigs:
            if "P_OV" in sig["flags"]:
                continue
            if sig["mode"] not in ("MOM", "MIX", "REV"):
                continue
            effective_score = sig["score"]
            sec_gap = sig.get("sector_gap", 0.0)
            if params.sector_penalty_pts != 0 and sec_gap < params.sector_penalty_threshold:
                effective_score += params.sector_penalty_pts
            elif params.sector_bonus_pts != 0 and sec_gap >= params.sector_bonus_threshold:
                effective_score += params.sector_bonus_pts
            if effective_score < params.invest_min_score:
                continue
            score_dist[sig["market"]][effective_score] += 1

    for market in sorted(score_dist.keys()):
        dist = score_dist[market]
        total = sum(dist.values())
        print(f"\n  {market} ({total}건):")
        for score in sorted(dist.keys()):
            cnt = dist[score]
            bar = "█" * (cnt * 40 // max(dist.values()))
            print(f"    {score:2d}점: {cnt:4d} ({100*cnt/total:5.1f}%) {bar}")


def analyze_flags_by_mode(cache: OptCache, params: Params) -> dict[str, dict[str, MarketStats]]:
    """모드별 플래그 성과 분석. {mode: {flag: MarketStats}}"""
    stats: dict[str, dict[str, MarketStats]] = defaultdict(lambda: defaultdict(MarketStats))

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
            elif params.sector_bonus_pts != 0 and sec_gap >= params.sector_bonus_threshold:
                effective_score += params.sector_bonus_pts
            if effective_score < params.invest_min_score:
                continue

            res = _simulate_one(sig, entry_date, cache.ohlcv_idx, params, hold_dates)
            if res is None:
                continue

            result_type, ret_pct = res
            is_win = result_type.startswith("익절") or ret_pct > 0
            is_loss = (result_type == "손절") or ret_pct < 0

            mode = sig["mode"]
            flags = set(sig["flags"].split(",")) if sig["flags"] else set()
            for f in flags:
                ms = stats[mode][f]
                ms.trades += 1
                ms.total_ret += ret_pct
                ms.ret_sq += ret_pct * ret_pct
                if is_win:
                    ms.wins += 1
                elif is_loss:
                    ms.losses += 1

    return dict(stats)


def _report_flags_by_mode(stats: dict[str, dict[str, MarketStats]]):
    _SUPPLY_FLAGS = {"F_TRN", "I_TRN", "F_C3", "I_C3", "F_1", "I_1", "FI", "P_OV"}

    for mode in ["MOM", "MIX", "REV"]:
        if mode not in stats:
            continue
        flags = stats[mode]
        print(f"\n{'=' * 60}")
        print(f"모드: {mode} — 플래그별 성과")
        print(f"{'=' * 60}")
        print(f"  {'플래그':<6} {'거래':>5} {'승률':>7} {'손실률':>7} {'평균수익':>9} {'손실편향':>8}")
        print(f"  {'-' * 50}")

        sorted_flags = sorted(
            [(f, ms) for f, ms in flags.items() if f not in _SUPPLY_FLAGS and ms.trades >= 5],
            key=lambda x: x[1].loss_rate - x[1].win_rate,
            reverse=True,
        )
        for f, ms in sorted_flags:
            loss_bias = ms.loss_rate - ms.win_rate
            marker = " ⚠️" if loss_bias > 15 else ""
            print(f"  {f:<6} {ms.trades:>5} {ms.win_rate:>6.1f}% {ms.loss_rate:>6.1f}% "
                  f"{ms.avg_ret:>+8.2f}% {loss_bias:>+7.1f}{marker}")


def analyze_mode_flag_combos(cache: OptCache, params: Params):
    """MOM/MIX 모드의 주요 플래그 조합 분석."""
    combo_stats: dict[str, dict[str, MarketStats]] = defaultdict(lambda: defaultdict(MarketStats))

    for base_date in cache.scan_dates:
        entry_date = cache.next_biz.get(base_date)
        if not entry_date:
            continue

        sigs = cache.signals.get(base_date, [])
        hold_dates = cache.forward_dates.get(entry_date, [entry_date])

        for sig in sigs:
            if "P_OV" in sig["flags"]:
                continue
            if sig["mode"] not in ("MOM", "MIX"):
                continue

            effective_score = sig["score"]
            sec_gap = sig.get("sector_gap", 0.0)
            if params.sector_penalty_pts != 0 and sec_gap < params.sector_penalty_threshold:
                effective_score += params.sector_penalty_pts
            elif params.sector_bonus_pts != 0 and sec_gap >= params.sector_bonus_threshold:
                effective_score += params.sector_bonus_pts
            if effective_score < params.invest_min_score:
                continue

            res = _simulate_one(sig, entry_date, cache.ohlcv_idx, params, hold_dates)
            if res is None:
                continue

            result_type, ret_pct = res
            is_win = result_type.startswith("익절") or ret_pct > 0

            mode = sig["mode"]
            # 수급 플래그 제외한 신호 플래그만
            _SUPPLY = {"F_TRN", "I_TRN", "F_C3", "I_C3", "F_1", "I_1", "FI", "P_OV"}
            flags = sorted(f for f in sig["flags"].split(",") if f and f not in _SUPPLY)
            combo_key = "+".join(flags)

            ms = combo_stats[mode][combo_key]
            ms.trades += 1
            ms.total_ret += ret_pct
            ms.ret_sq += ret_pct * ret_pct
            if is_win:
                ms.wins += 1
            elif (result_type == "손절") or ret_pct < 0:
                ms.losses += 1

    for mode in ["MOM", "MIX"]:
        if mode not in combo_stats:
            continue
        print(f"\n{'=' * 70}")
        print(f"모드: {mode} — 플래그 조합별 성과 (거래 2건 이상)")
        print(f"{'=' * 70}")
        print(f"  {'조합':<30} {'거래':>5} {'승률':>7} {'평균수익':>9} {'총수익':>9}")
        print(f"  {'-' * 65}")

        combos = sorted(
            [(k, ms) for k, ms in combo_stats[mode].items() if ms.trades >= 2],
            key=lambda x: x[1].trades,
            reverse=True,
        )
        for combo, ms in combos[:20]:
            print(f"  {combo:<30} {ms.trades:>5} {ms.win_rate:>6.1f}% "
                  f"{ms.avg_ret:>+8.2f}% {ms.total_ret:>+8.1f}%")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    parser = argparse.ArgumentParser()
    parser.add_argument("--years", type=int, default=2)
    args = parser.parse_args()

    end_date = dtutils.today()
    start_date = dtutils.add_days(end_date, -365 * args.years)

    log.info(f"기간: {start_date} ~ {end_date} ({args.years}년)")
    cache = build_or_load(start_date, end_date)
    _gs._WORKER_CACHE = cache  # _simulate_one이 참조하는 글로벌 캐시 설정

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
        sector_bonus_threshold=_f.SECTOR_BONUS_THRESHOLD,
        sector_bonus_pts=_f.SECTOR_BONUS_PTS,
    )
    log.info(f"파라미터: {params.label()}")

    # 시장별 분석
    market_stats = analyze_by_market(cache, params)
    _report(market_stats)

    # 시장 × 모드 교차 분석
    mm_stats = analyze_by_market_mode(cache, params)
    _report_market_mode(mm_stats)

    # 모드별 플래그 성과
    flag_mode_stats = analyze_flags_by_mode(cache, params)
    _report_flags_by_mode(flag_mode_stats)

    # MOM/MIX 플래그 조합 분석
    analyze_mode_flag_combos(cache, params)

    # 점수 분포
    _report_score_distribution(cache, params, market_stats)
