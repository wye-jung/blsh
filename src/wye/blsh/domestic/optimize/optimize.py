"""파라미터 최적화 스크립트"""

import logging
from wye.blsh.database import select_all
from wye.blsh.domestic import scanner
from wye.blsh.domestic import simulator, _factor as fac

logging.basicConfig(level=logging.WARNING, format="%(message)s")


def _get_biz_dates(from_date):
    rows = select_all(
        "SELECT DISTINCT trd_dd FROM isu_ksp_ohlcv WHERE trd_dd >= :fd ORDER BY trd_dd",
        fd=from_date,
    )
    return [r["trd_dd"] for r in rows]


def _portfolio_stats(batch_results, biz_dates, cash_usage, initial_capital=10_000_000, min_alloc=10_000):
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
        new_entries = [r for r in entries_by_date.get(date, []) if r["ticker"] not in open_tickers]
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
                cash += pos["allocated"] * (1 + ret_pct / 100)
                all_trades.append({"result_type": pos.get("result_type", ""), "ret_pct": ret_pct})
            else:
                still_open.append(pos)
        open_positions = still_open

    unrealized = sum(p["allocated"] for p in open_positions)
    total_ret = (cash + unrealized - initial_capital) / initial_capital * 100

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


def build_scan_cache(from_date):
    """
    전체 날짜를 1회만 스캔해 캐시로 반환.
    INVEST_MIN_SCORE=1 로 낮춰 모든 신호를 저장 (필터는 run_backtest_cached 에서 재적용).
    반환: (cache, biz_dates)
      cache: {base_date: {"signals": DataFrame, "target_date": str}}
    """
    orig = fac.INVEST_MIN_SCORE
    fac.INVEST_MIN_SCORE = 1

    biz_dates = _get_biz_dates(from_date)
    cache = {}

    print(f"캐시 빌드: {len(biz_dates)}개 날짜 스캔 중...", flush=True)
    for i, base_date in enumerate(biz_dates, 1):
        try:
            candidates, target_date, _ = scanner.scan(base_date)
            if not candidates.empty and target_date:
                cache[base_date] = {"signals": candidates, "target_date": target_date}
        except Exception:
            continue
        if i % 10 == 0:
            print(f"  {i}/{len(biz_dates)} 완료", flush=True)

    fac.INVEST_MIN_SCORE = orig
    print(f"캐시 완료: {len(cache)}개 날짜 유효\n", flush=True)
    return cache, biz_dates


def run_backtest_cached(cache, biz_dates, params):
    """
    캐시된 스캔 결과에 params 적용 → 빠른 백테스트.
    스캔을 다시 수행하지 않으므로 각 호출이 수 초 이내.
    """
    invest_min = params.get("INVEST_MIN_SCORE", fac.INVEST_MIN_SCORE)
    sl_mult    = params.get("ATR_SL_MULT",      fac.ATR_SL_MULT)
    tp_mult    = params.get("ATR_TP_MULT",      fac.ATR_TP_MULT)
    cash_usage = params.get("CASH_USAGE", 0.9)

    # simulator 가 fac.MAX_HOLD_DAYS* 를 직접 읽으므로 패치
    for k in ("MAX_HOLD_DAYS", "MAX_HOLD_DAYS_MIX", "MAX_HOLD_DAYS_MOM"):
        if k in params and hasattr(fac, k):
            setattr(fac, k, params[k])

    batch_results = {}
    for base_date, cached in cache.items():
        df = cached["signals"].copy()
        target_date = cached["target_date"]

        # INVEST_MIN_SCORE 필터 재적용
        mask = (
            (df["buy_score"] >= invest_min)
            & (df["mode"].isin(["MIX", "MOM", "REV"]))
            & (~df["buy_flags"].str.contains("P_OV", na=False))
        )
        candidates = df[mask].copy()
        if candidates.empty:
            continue

        # ATR 멀티플라이어로 entry/SL/TP 재계산
        candidates["entry_price"] = (candidates["close"] + 0.5  * candidates["atr"]).round(2)
        candidates["stop_loss"]   = (candidates["close"] - sl_mult * candidates["atr"]).round(2)
        candidates["take_profit"] = (candidates["close"] + tp_mult * candidates["atr"]).round(2)

        try:
            ret = simulator.simulate(candidates, target_date)
        except Exception:
            continue
        if ret is None:
            continue
        rows_ok, _, _ = ret
        if rows_ok:
            batch_results.setdefault(target_date, []).extend(rows_ok)

    return _portfolio_stats(batch_results, biz_dates, cash_usage)


def run_backtest(params: dict, from_date="20250901") -> dict:
    """기존 호환용: 매번 전체 스캔 수행 (느림). 단건 확인 용도."""
    for k, v in params.items():
        if hasattr(fac, k):
            setattr(fac, k, v)

    cash_usage = params.get("CASH_USAGE", 0.9)
    biz_dates = _get_biz_dates(from_date)

    batch_results = {}
    for base_date in biz_dates:
        try:
            candidates, target_date, _ = scanner.scan(base_date)
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

    return _portfolio_stats(batch_results, biz_dates, cash_usage)


if __name__ == "__main__":
    FROM_DATE = "20250915"
    cache, biz_dates = build_scan_cache(FROM_DATE)

    def bt(p):
        return run_backtest_cached(cache, biz_dates, p)

    baseline = {
        "INVEST_MIN_SCORE": 7,
        "ATR_SL_MULT": 1.5,
        "ATR_TP_MULT": 3.0,
        "MAX_HOLD_DAYS": 5,
        "MAX_HOLD_DAYS_MIX": 3,
        "MAX_HOLD_DAYS_MOM": 2,
    }
    print("=== 베이스라인 ===")
    r = bt(baseline)
    print(r)
    best_params = dict(baseline)

    # ── 라운드 1: INVEST_MIN_SCORE
    print("\n=== R1: INVEST_MIN_SCORE ===")
    r1 = [(baseline["INVEST_MIN_SCORE"], r)]
    for score in [5, 6, 8, 9]:
        p = {**best_params, "INVEST_MIN_SCORE": score}
        res = bt(p)
        r1.append((score, res))
        print(f"  score={score}: {res}")
    best_r1 = max(r1, key=lambda x: x[1]["total_ret"])
    best_params["INVEST_MIN_SCORE"] = best_r1[0]
    print(f"\n>> 최적 INVEST_MIN_SCORE={best_r1[0]}: {best_r1[1]}")

    # ── 라운드 2: ATR_TP_MULT
    print("\n=== R2: ATR_TP_MULT ===")
    r2 = [(baseline["ATR_TP_MULT"], bt(dict(best_params)))]
    for tp in [2.0, 2.5, 3.5, 4.0]:
        p = {**best_params, "ATR_TP_MULT": tp}
        res = bt(p)
        r2.append((tp, res))
        print(f"  tp={tp}: {res}")
    best_r2 = max(r2, key=lambda x: x[1]["total_ret"])
    best_params["ATR_TP_MULT"] = best_r2[0]
    print(f"\n>> 최적 ATR_TP_MULT={best_r2[0]}: {best_r2[1]}")

    # ── 라운드 3: ATR_SL_MULT
    print("\n=== R3: ATR_SL_MULT ===")
    r3 = [(baseline["ATR_SL_MULT"], bt(dict(best_params)))]
    for sl in [1.0, 2.0, 2.5]:
        p = {**best_params, "ATR_SL_MULT": sl}
        res = bt(p)
        r3.append((sl, res))
        print(f"  sl={sl}: {res}")
    best_r3 = max(r3, key=lambda x: x[1]["total_ret"])
    best_params["ATR_SL_MULT"] = best_r3[0]
    print(f"\n>> 최적 ATR_SL_MULT={best_r3[0]}: {best_r3[1]}")

    # ── 라운드 4: MAX_HOLD_DAYS
    print("\n=== R4: MAX_HOLD_DAYS ===")
    hold_combos = [(3, 2, 1), (7, 4, 2), (10, 5, 3)]
    r4 = [((5, 3, 2), bt({**best_params, "MAX_HOLD_DAYS": 5, "MAX_HOLD_DAYS_MIX": 3, "MAX_HOLD_DAYS_MOM": 2}))]
    for rev, mix, mom in hold_combos:
        p = {**best_params, "MAX_HOLD_DAYS": rev, "MAX_HOLD_DAYS_MIX": mix, "MAX_HOLD_DAYS_MOM": mom}
        res = bt(p)
        r4.append(((rev, mix, mom), res))
        print(f"  (REV={rev},MIX={mix},MOM={mom}): {res}")
    best_r4 = max(r4, key=lambda x: x[1]["total_ret"])
    best_params["MAX_HOLD_DAYS"]     = best_r4[0][0]
    best_params["MAX_HOLD_DAYS_MIX"] = best_r4[0][1]
    best_params["MAX_HOLD_DAYS_MOM"] = best_r4[0][2]
    print(f"\n>> 최적 HOLD_DAYS={best_r4[0]}: {best_r4[1]}")

    print("\n=== 최종 최적 파라미터 ===")
    print(best_params)
    print(f"최종 결과: {bt(best_params)}")
