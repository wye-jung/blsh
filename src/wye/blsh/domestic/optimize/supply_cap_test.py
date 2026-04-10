"""
수급 가산 상한(Supply Cap) 백테스트 비교
──────────────────────────────────────
cap=None: 수급 무제한
cap=N   : 수급 가산 최대 +N

실행:
    uv run python -m wye.blsh.domestic.optimize.supply_cap_test
    uv run python -m wye.blsh.domestic.optimize.supply_cap_test --years 2
"""

import argparse
import logging

from wye.blsh.common import dtutils
from wye.blsh.domestic.optimize._cache import build_or_load
from wye.blsh.domestic.optimize import grid_search as _gs
from wye.blsh.domestic.optimize.grid_search import Params, Stats, _simulate_one

log = logging.getLogger(__name__)


def backtest_capped(cache, params: Params, supply_cap: int | None = None) -> Stats:
    """supply_cap=None → 수급 무제한, supply_cap=N → 수급 가산 최대 +N."""
    st = Stats()

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

            ts = sig.get("tech_score")
            raw_bonus = sig.get("raw_supply_bonus")
            if ts is None or raw_bonus is None:
                # 구형 캐시 fallback (--rebuild 전)
                log.warning("캐시에 tech_score/raw_supply_bonus 없음 → --rebuild 필요")
                return st

            if supply_cap is None:
                base_score = ts + raw_bonus
            else:
                base_score = ts + min(raw_bonus, supply_cap)

            if base_score < params.invest_min_score:
                continue

            res = _simulate_one(sig, entry_date, cache.ohlcv_idx, params, hold_dates)
            if res is None:
                continue

            result_type, ret_pct = res
            st.trades += 1
            st.total_ret += ret_pct
            st.ret_sq += ret_pct * ret_pct
            if result_type.startswith("익절"):
                st.wins += 1
            elif result_type == "손절":
                st.losses += 1
            else:
                st.holds += 1

    return st


def run(years: int = 2):
    end_date = dtutils.today()
    start_date = dtutils.add_days(end_date, -years * 365)
    log.info(f"기간: {start_date} ~ {end_date} ({years}년)")

    cache = build_or_load(start_date, end_date)
    _gs._WORKER_CACHE = cache

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
    )
    log.info(f"파라미터: {params.label()}")

    cases = [
        ("무제한 (cap없음)", None),
        ("cap +7",          7),
        ("cap +5",          5),
        ("cap +3 (현행)",   3),
        ("cap +2",          2),
        ("cap +1",          1),
    ]

    results = []
    for label, cap in cases:
        st = backtest_capped(cache, params, supply_cap=cap)
        if st.trades == 0 and cap is None:
            # 구형 캐시 감지
            break
        results.append((label, cap, st))

    if not results:
        log.error("캐시에 tech_score/raw_supply_bonus 없음 → --rebuild 후 재실행")
        return

    log.info("")
    log.info("=" * 100)
    log.info("  수급 캡 비교 결과")
    log.info("=" * 100)
    log.info(
        f"  {'케이스':<20s}  {'거래':>6s}  {'승률':>6s}  {'손절률':>6s}  "
        f"{'평균수익':>8s}  {'총수익':>10s}  {'metric':>8s}"
    )
    log.info("-" * 100)
    for label, cap, st in results:
        loss_rate = st.losses / st.trades * 100 if st.trades else 0
        log.info(
            f"  {label:<20s}  {st.trades:>5d}건  {st.win_rate:>5.1f}%  "
            f"{loss_rate:>5.1f}%  {st.avg_ret:>+7.2f}%  "
            f"{st.total_ret:>+9.1f}%  {st.metric:>8.1f}"
        )
    log.info("=" * 100)

    # 현행(cap=3) 대비 거래 증감
    base = next((st for _, c, st in results if c == 3), None)
    if base and base.trades:
        log.info("\n  [ 현행(cap=3) 대비 비교 ]")
        log.info(f"  {'케이스':<20s}  {'거래 증감':>8s}  {'총수익 증감':>10s}")
        log.info("-" * 50)
        for label, cap, st in results:
            if cap == 3:
                continue
            delta_t = st.trades - base.trades
            delta_r = st.total_ret - base.total_ret
            log.info(f"  {label:<20s}  {delta_t:>+7d}건  {delta_r:>+9.1f}%")
        log.info("=" * 100)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="수급 캡 백테스트 비교")
    parser.add_argument("--years", type=int, default=2, help="백테스트 기간 (년, 기본 2)")
    args = parser.parse_args()

    run(years=args.years)
