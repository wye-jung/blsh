"""파라미터 최적화 스크립트"""

import logging
from blsh.database import select_all
from blsh.wye.domestic import scanner, simulator, _factor as fac

logging.basicConfig(level=logging.WARNING, format="%(message)s")


def run_backtest(params: dict, from_date="20250901") -> dict:
    """
    params: dict with keys matching _factor attributes + optional backtest keys
      e.g. {"INVEST_MIN_SCORE": 7, "ATR_SL_MULT": 1.5, ...}
    Returns: {"total_ret": float, "win_rate": float, "n_trades": int}
    """
    # monkey-patch fac
    for k, v in params.items():
        if hasattr(fac, k):
            setattr(fac, k, v)

    cash_usage = params.get("CASH_USAGE", 0.9)
    min_alloc = 10_000
    initial_capital = 10_000_000

    rows_db = select_all(
        "SELECT DISTINCT trd_dd FROM isu_ksp_ohlcv WHERE trd_dd >= :fd ORDER BY trd_dd",
        fd=from_date,
    )
    biz_dates = [r["trd_dd"] for r in rows_db]

    batch_results = {}
    for base_date in biz_dates:
        try:
            candidates, target_date, bd = scanner.scan(base_date)
        except Exception:
            continue
        if candidates.empty or not target_date:
            continue

        try:
            ret = simulator.simulate(candidates, target_date)
        except Exception:
            continue
        if ret is None:
            continue
        rows_ok, _, _ = ret
        if rows_ok:
            batch_results.setdefault(target_date, []).extend(rows_ok)

    # portfolio tracking
    cash = float(initial_capital)
    open_positions = []
    all_trades = []

    entries_by_date = {}
    for rows in batch_results.values():
        for r in rows:
            ed = r.get("entry_date")
            if ed:
                entries_by_date.setdefault(ed, []).append(r)

    for date in biz_dates:
        open_tickers = {p["ticker"] for p in open_positions}
        new_entries = [
            r for r in entries_by_date.get(date, []) if r["ticker"] not in open_tickers
        ]
        if new_entries:
            avail = cash * cash_usage
            alloc = avail / len(new_entries)
            if alloc >= min_alloc:
                for r in new_entries:
                    cash -= alloc
                    open_positions.append({**r, "allocated": alloc})

        still_open = []
        for pos in open_positions:
            if pos.get("exit_date") == date:
                ret_pct = float(pos.get("ret_pct", 0))
                exit_val = pos["allocated"] * (1 + ret_pct / 100)
                cash += exit_val
                all_trades.append(
                    {
                        "result_type": pos.get("result_type", ""),
                        "ret_pct": ret_pct,
                    }
                )
            else:
                still_open.append(pos)
        open_positions = still_open

    unrealized = sum(p["allocated"] for p in open_positions)
    final_val = cash + unrealized
    total_ret = (final_val - initial_capital) / initial_capital * 100

    n_wins = sum(1 for t in all_trades if t["result_type"] == "익절")
    n_losses = sum(1 for t in all_trades if t["result_type"] == "손절")
    decisive = n_wins + n_losses
    win_rate = n_wins / decisive * 100 if decisive else 0

    return {
        "total_ret": round(total_ret, 2),
        "win_rate": round(win_rate, 2),
        "n_trades": len(all_trades),
        "n_wins": n_wins,
        "n_losses": n_losses,
    }


if __name__ == "__main__":
    import sys

    # 베이스라인
    baseline = {
        "INVEST_MIN_SCORE": 7,
        "ATR_SL_MULT": 1.5,
        "ATR_TP_MULT": 3.0,
        "MAX_HOLD_DAYS": 5,
        "MAX_HOLD_DAYS_MIX": 3,
        "MAX_HOLD_DAYS_MOM": 2,
    }
    print("=== 베이스라인 ===")
    r = run_backtest(baseline)
    print(r)

    best_score = r["total_ret"]
    best_params = dict(baseline)

    # ── 라운드 1: INVEST_MIN_SCORE 탐색
    print("\n=== 라운드 1: INVEST_MIN_SCORE 탐색 ===")
    round1_results = []
    for score in [5, 6, 8, 9]:
        p = {**best_params, "INVEST_MIN_SCORE": score}
        res = run_backtest(p)
        round1_results.append((score, res))
        print(f"  INVEST_MIN_SCORE={score}: {res}")

    # 최적 score 선택
    all_r1 = [(baseline["INVEST_MIN_SCORE"], r)] + [
        (s, res) for s, res in round1_results
    ]
    best_r1 = max(all_r1, key=lambda x: x[1]["total_ret"])
    best_params["INVEST_MIN_SCORE"] = best_r1[0]
    print(f"\n>> R1 최적 INVEST_MIN_SCORE={best_r1[0]}: {best_r1[1]}")

    # ── 라운드 2: ATR_TP_MULT 탐색
    print("\n=== 라운드 2: ATR_TP_MULT 탐색 ===")
    round2_results = []
    for tp in [2.0, 2.5, 3.5, 4.0]:
        p = {**best_params, "ATR_TP_MULT": tp}
        res = run_backtest(p)
        round2_results.append((tp, res))
        print(f"  ATR_TP_MULT={tp}: {res}")

    all_r2 = [
        (baseline["ATR_TP_MULT"], run_backtest({**best_params, "ATR_TP_MULT": 3.0}))
    ] + [(tp, res) for tp, res in round2_results]
    best_r2 = max(all_r2, key=lambda x: x[1]["total_ret"])
    best_params["ATR_TP_MULT"] = best_r2[0]
    print(f"\n>> R2 최적 ATR_TP_MULT={best_r2[0]}: {best_r2[1]}")

    # ── 라운드 3: ATR_SL_MULT 탐색
    print("\n=== 라운드 3: ATR_SL_MULT 탐색 ===")
    round3_results = []
    for sl in [1.0, 2.0, 2.5]:
        p = {**best_params, "ATR_SL_MULT": sl}
        res = run_backtest(p)
        round3_results.append((sl, res))
        print(f"  ATR_SL_MULT={sl}: {res}")

    all_r3 = [
        (baseline["ATR_SL_MULT"], run_backtest({**best_params, "ATR_SL_MULT": 1.5}))
    ] + [(sl, res) for sl, res in round3_results]
    best_r3 = max(all_r3, key=lambda x: x[1]["total_ret"])
    best_params["ATR_SL_MULT"] = best_r3[0]
    print(f"\n>> R3 최적 ATR_SL_MULT={best_r3[0]}: {best_r3[1]}")

    # ── 라운드 4: MAX_HOLD_DAYS 탐색
    print("\n=== 라운드 4: MAX_HOLD_DAYS 탐색 ===")
    hold_combos = [
        (3, 2, 1),
        (7, 4, 2),
        (10, 5, 3),
    ]
    round4_results = []
    for rev, mix, mom in hold_combos:
        p = {
            **best_params,
            "MAX_HOLD_DAYS": rev,
            "MAX_HOLD_DAYS_MIX": mix,
            "MAX_HOLD_DAYS_MOM": mom,
        }
        res = run_backtest(p)
        round4_results.append(((rev, mix, mom), res))
        print(f"  (REV={rev},MIX={mix},MOM={mom}): {res}")

    baseline_hold = run_backtest(
        {
            **best_params,
            "MAX_HOLD_DAYS": 5,
            "MAX_HOLD_DAYS_MIX": 3,
            "MAX_HOLD_DAYS_MOM": 2,
        }
    )
    all_r4 = [((5, 3, 2), baseline_hold)] + round4_results
    best_r4 = max(all_r4, key=lambda x: x[1]["total_ret"])
    best_params["MAX_HOLD_DAYS"] = best_r4[0][0]
    best_params["MAX_HOLD_DAYS_MIX"] = best_r4[0][1]
    best_params["MAX_HOLD_DAYS_MOM"] = best_r4[0][2]
    print(f"\n>> R4 최적 HOLD_DAYS={best_r4[0]}: {best_r4[1]}")

    print("\n=== 최종 최적 파라미터 ===")
    print(best_params)
    final_res = run_backtest(best_params)
    print(f"최종 결과: {final_res}")
