"""데이 트레이딩 파라미터 최적화 (MAX_HOLD_DAYS=1 고정)"""
import logging
from blsh.wye.domestic.optimize import run_backtest

logging.basicConfig(level=logging.WARNING, format="%(message)s")

# MAX_HOLD_DAYS=1 고정, 나머지 최적화 탐색
BASE = {
    "INVEST_MIN_SCORE": 9,
    "ATR_SL_MULT": 2.0,
    "ATR_TP_MULT": 2.0,
    "MAX_HOLD_DAYS": 1,
    "MAX_HOLD_DAYS_MIX": 1,
    "MAX_HOLD_DAYS_MOM": 1,
}

if __name__ == "__main__":
    print("=== 데이 트레이딩 베이스라인 (HOLD=1) ===")
    base_res = run_backtest(dict(BASE))
    print(base_res)
    best_params = dict(BASE)

    # ── 라운드 1: INVEST_MIN_SCORE 탐색
    print("\n=== 라운드 1: INVEST_MIN_SCORE 탐색 ===")
    r1 = [(BASE["INVEST_MIN_SCORE"], base_res)]
    for score in [6, 7, 8, 10, 11]:
        p = {**best_params, "INVEST_MIN_SCORE": score}
        res = run_backtest(p)
        r1.append((score, res))
        print(f"  INVEST_MIN_SCORE={score}: {res}")
    best_r1 = max(r1, key=lambda x: x[1]["total_ret"])
    best_params["INVEST_MIN_SCORE"] = best_r1[0]
    print(f"\n>> R1 최적 INVEST_MIN_SCORE={best_r1[0]}: {best_r1[1]}")

    # ── 라운드 2: ATR_TP_MULT 탐색
    print("\n=== 라운드 2: ATR_TP_MULT 탐색 ===")
    r2 = [(BASE["ATR_TP_MULT"], run_backtest(dict(best_params)))]
    for tp in [1.0, 1.5, 2.5, 3.0, 3.5]:
        p = {**best_params, "ATR_TP_MULT": tp}
        res = run_backtest(p)
        r2.append((tp, res))
        print(f"  ATR_TP_MULT={tp}: {res}")
    best_r2 = max(r2, key=lambda x: x[1]["total_ret"])
    best_params["ATR_TP_MULT"] = best_r2[0]
    print(f"\n>> R2 최적 ATR_TP_MULT={best_r2[0]}: {best_r2[1]}")

    # ── 라운드 3: ATR_SL_MULT 탐색
    print("\n=== 라운드 3: ATR_SL_MULT 탐색 ===")
    r3 = [(BASE["ATR_SL_MULT"], run_backtest(dict(best_params)))]
    for sl in [0.5, 1.0, 1.5, 2.5, 3.0]:
        p = {**best_params, "ATR_SL_MULT": sl}
        res = run_backtest(p)
        r3.append((sl, res))
        print(f"  ATR_SL_MULT={sl}: {res}")
    best_r3 = max(r3, key=lambda x: x[1]["total_ret"])
    best_params["ATR_SL_MULT"] = best_r3[0]
    print(f"\n>> R3 최적 ATR_SL_MULT={best_r3[0]}: {best_r3[1]}")

    # ── 라운드 4: CASH_USAGE 탐색 (데이 트레이딩은 집중도 조정)
    print("\n=== 라운드 4: CASH_USAGE 탐색 ===")
    r4 = [(0.9, run_backtest(dict(best_params)))]
    for cu in [0.5, 0.7, 1.0]:
        p = {**best_params, "CASH_USAGE": cu}
        res = run_backtest(p)
        r4.append((cu, res))
        print(f"  CASH_USAGE={cu}: {res}")
    best_r4 = max(r4, key=lambda x: x[1]["total_ret"])
    best_params["CASH_USAGE"] = best_r4[0]
    print(f"\n>> R4 최적 CASH_USAGE={best_r4[0]}: {best_r4[1]}")

    print("\n=== 최종 최적 파라미터 (데이 트레이딩) ===")
    print(best_params)
    final_res = run_backtest(dict(best_params))
    print(f"최종 결과: {final_res}")
