"""스윙 트레이딩 파라미터 최적화"""
import logging
from blsh.wye.domestic.optimize import build_scan_cache, run_backtest_cached
from blsh.wye.domestic import _factor as fac

logging.basicConfig(level=logging.WARNING, format="%(message)s")

FROM_DATE = "20250315"  # 최근 1년

BASE = {
    "INVEST_MIN_SCORE": 9,
    "ATR_SL_MULT": 2.0,
    "ATR_TP_MULT": 2.0,
    "MAX_HOLD_DAYS": 10,
    "MAX_HOLD_DAYS_MIX": 5,
    "MAX_HOLD_DAYS_MOM": 3,
}

if __name__ == "__main__":
    # SWING 모드로 전환
    fac.TRADE_FLAG = "SWING"

    cache, biz_dates = build_scan_cache(FROM_DATE)

    def bt(p):
        return run_backtest_cached(cache, biz_dates, p)

    print(f"=== 스윙 트레이딩 최적화  |  기간: {FROM_DATE}~  |  거래일: {len(biz_dates)}일 ===\n")

    print("── 베이스라인 ──")
    base_res = bt(dict(BASE))
    print(base_res)
    best_params = dict(BASE)

    # ── R1: INVEST_MIN_SCORE
    print("\n=== R1: INVEST_MIN_SCORE ===")
    r1 = [(BASE["INVEST_MIN_SCORE"], base_res)]
    for score in [5, 6, 7, 8, 10, 11, 12]:
        p = {**best_params, "INVEST_MIN_SCORE": score}
        res = bt(p)
        r1.append((score, res))
        print(f"  score={score:2d}: {res}")
    best_r1 = max(r1, key=lambda x: x[1]["total_ret"])
    best_params["INVEST_MIN_SCORE"] = best_r1[0]
    print(f"\n>> 최적 INVEST_MIN_SCORE={best_r1[0]}: {best_r1[1]}")

    # ── R2: ATR_TP_MULT
    print("\n=== R2: ATR_TP_MULT ===")
    r2 = [(BASE["ATR_TP_MULT"], bt(dict(best_params)))]
    for tp in [1.0, 1.5, 2.5, 3.0, 3.5, 4.0]:
        p = {**best_params, "ATR_TP_MULT": tp}
        res = bt(p)
        r2.append((tp, res))
        print(f"  tp={tp}: {res}")
    best_r2 = max(r2, key=lambda x: x[1]["total_ret"])
    best_params["ATR_TP_MULT"] = best_r2[0]
    print(f"\n>> 최적 ATR_TP_MULT={best_r2[0]}: {best_r2[1]}")

    # ── R3: ATR_SL_MULT
    print("\n=== R3: ATR_SL_MULT ===")
    r3 = [(BASE["ATR_SL_MULT"], bt(dict(best_params)))]
    for sl in [0.5, 1.0, 1.5, 2.5, 3.0]:
        p = {**best_params, "ATR_SL_MULT": sl}
        res = bt(p)
        r3.append((sl, res))
        print(f"  sl={sl}: {res}")
    best_r3 = max(r3, key=lambda x: x[1]["total_ret"])
    best_params["ATR_SL_MULT"] = best_r3[0]
    print(f"\n>> 최적 ATR_SL_MULT={best_r3[0]}: {best_r3[1]}")

    # ── R4: MAX_HOLD_DAYS (REV / MIX / MOM 개별)
    print("\n=== R4: MAX_HOLD_DAYS ===")
    hold_combos = [
        (3, 2, 1),
        (5, 3, 2),
        (7, 4, 2),
        (10, 5, 3),
        (15, 7, 3),
    ]
    r4 = []
    for rev, mix, mom in hold_combos:
        p = {**best_params, "MAX_HOLD_DAYS": rev, "MAX_HOLD_DAYS_MIX": mix, "MAX_HOLD_DAYS_MOM": mom}
        res = bt(p)
        r4.append(((rev, mix, mom), res))
        print(f"  (REV={rev}, MIX={mix}, MOM={mom}): {res}")
    best_r4 = max(r4, key=lambda x: x[1]["total_ret"])
    best_params["MAX_HOLD_DAYS"]     = best_r4[0][0]
    best_params["MAX_HOLD_DAYS_MIX"] = best_r4[0][1]
    best_params["MAX_HOLD_DAYS_MOM"] = best_r4[0][2]
    print(f"\n>> 최적 HOLD_DAYS={best_r4[0]}: {best_r4[1]}")

    # ── R5: CASH_USAGE
    print("\n=== R5: CASH_USAGE ===")
    r5 = [(0.9, bt(dict(best_params)))]
    for cu in [0.3, 0.5, 0.7, 1.0]:
        p = {**best_params, "CASH_USAGE": cu}
        res = bt(p)
        r5.append((cu, res))
        print(f"  usage={cu}: {res}")
    best_r5 = max(r5, key=lambda x: x[1]["total_ret"])
    best_params["CASH_USAGE"] = best_r5[0]
    print(f"\n>> 최적 CASH_USAGE={best_r5[0]}: {best_r5[1]}")

    print("\n" + "=" * 60)
    print("최종 최적 파라미터 (스윙 트레이딩)")
    print("=" * 60)
    for k, v in best_params.items():
        print(f"  {k} = {v}")
    print(f"\n최종 결과: {bt(dict(best_params))}")
