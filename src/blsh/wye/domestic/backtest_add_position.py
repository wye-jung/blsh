"""
추가 매수 허용 vs 미허용 백테스트 비교 (스윙 트레이딩)

현재 trader.py 는 이미 보유 중인 종목이 다시 매수 신호로 나와도 스킵함.
이 스크립트는 스킵하지 않고 추가 매수했을 때 수익률 차이를 비교함.

가정:
  - 추가 매수 시 새 신호의 buy_price / SL / TP 를 그대로 사용 (독립 포지션으로 취급)
  - 평단가 기반 SL/TP 재계산은 시뮬레이터 구조상 어려우므로 보수적 근사
"""

import logging

from blsh.wye.domestic import _factor as fac, simulator
from blsh.wye.domestic.optimize import build_scan_cache, _portfolio_stats

logging.basicConfig(level=logging.WARNING, format="%(message)s")

FROM_DATE = "20250315"  # optimize_swing.py 와 동일 기간 (최근 1년)

SWING_PARAMS = {
    "INVEST_MIN_SCORE": 11,
    "ATR_SL_MULT": 2.0,
    "ATR_TP_MULT": 2.0,
    "MAX_HOLD_DAYS": 7,
    "MAX_HOLD_DAYS_MIX": 4,
    "MAX_HOLD_DAYS_MOM": 2,
    "CASH_USAGE": 0.9,
}


# ─────────────────────────────────────────
# 추가 매수 허용 버전 _portfolio_stats
# ─────────────────────────────────────────
def _portfolio_stats_allow_add(
    batch_results,
    biz_dates,
    cash_usage,
    initial_capital=10_000_000,
    min_alloc=10_000,
):
    """open_tickers 필터 제거 → 이미 보유 중인 종목도 재매수."""
    cash = float(initial_capital)
    open_positions = []
    all_trades = []

    entries_by_date: dict[str, list] = {}
    for rows in batch_results.values():
        for r in rows:
            ed = r.get("entry_date")
            if ed:
                entries_by_date.setdefault(ed, []).append(r)

    for date in biz_dates:
        # ★ 차이점: 기존 보유 종목 필터 없음
        new_entries = entries_by_date.get(date, [])
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
                all_trades.append(
                    {"result_type": pos.get("result_type", ""), "ret_pct": ret_pct}
                )
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


# ─────────────────────────────────────────
# 공통 백테스트 실행 (stats_fn 교체 가능)
# ─────────────────────────────────────────
def _run(cache, biz_dates, params, stats_fn):
    invest_min = params.get("INVEST_MIN_SCORE", fac.INVEST_MIN_SCORE)
    sl_mult = params.get("ATR_SL_MULT", fac.ATR_SL_MULT)
    tp_mult = params.get("ATR_TP_MULT", fac.ATR_TP_MULT)
    cash_usage = params.get("CASH_USAGE", 0.9)

    for k in ("MAX_HOLD_DAYS", "MAX_HOLD_DAYS_MIX", "MAX_HOLD_DAYS_MOM"):
        if k in params and hasattr(fac, k):
            setattr(fac, k, params[k])

    batch_results: dict[str, list] = {}
    for base_date, cached in cache.items():
        df = cached["signals"].copy()
        target_date = cached["target_date"]

        mask = (
            (df["buy_score"] >= invest_min)
            & (df["mode"].isin(["MIX", "MOM", "REV"]))
            & (~df["buy_flags"].str.contains("P_OV", na=False))
        )
        candidates = df[mask].copy()
        if candidates.empty:
            continue

        candidates["entry_price"] = (
            candidates["close"] + 0.5 * candidates["atr"]
        ).round(2)
        candidates["stop_loss"] = (
            candidates["close"] - sl_mult * candidates["atr"]
        ).round(2)
        candidates["take_profit"] = (
            candidates["close"] + tp_mult * candidates["atr"]
        ).round(2)

        try:
            ret = simulator.simulate(candidates, target_date)
        except Exception:
            continue
        if ret is None:
            continue
        rows_ok, _, _ = ret
        if rows_ok:
            batch_results.setdefault(target_date, []).extend(rows_ok)

    return stats_fn(batch_results, biz_dates, cash_usage)


# ─────────────────────────────────────────
# 진입점
# ─────────────────────────────────────────
if __name__ == "__main__":
    fac.TRADE_FLAG = "SWING"
    for k, v in SWING_PARAMS.items():
        if hasattr(fac, k):
            setattr(fac, k, v)

    print(f"=== 추가 매수 허용 vs 미허용  |  기간: {FROM_DATE}~ ===\n")

    cache, biz_dates = build_scan_cache(FROM_DATE)
    print(f"거래일: {len(biz_dates)}일\n")

    print("── 기준: 추가 매수 미허용 (현재 trader.py 동작) ──")
    no_add = _run(cache, biz_dates, SWING_PARAMS, _portfolio_stats)
    print(f"  {no_add}")

    print("\n── 비교: 추가 매수 허용 ──")
    allow_add = _run(cache, biz_dates, SWING_PARAMS, _portfolio_stats_allow_add)
    print(f"  {allow_add}")

    print("\n" + "=" * 55)
    print("결과 비교")
    print("=" * 55)
    print(f"{'항목':<12}  {'미허용':>10}  {'허용':>10}  {'차이':>10}")
    print("-" * 55)
    print(
        f"{'수익률(%)':<12}  {no_add['total_ret']:>10.2f}  {allow_add['total_ret']:>10.2f}"
        f"  {allow_add['total_ret'] - no_add['total_ret']:>+10.2f}"
    )
    print(
        f"{'승률(%)':<12}  {no_add['win_rate']:>10.1f}  {allow_add['win_rate']:>10.1f}"
        f"  {allow_add['win_rate'] - no_add['win_rate']:>+10.1f}"
    )
    print(
        f"{'거래수':<12}  {no_add['n_trades']:>10}  {allow_add['n_trades']:>10}"
        f"  {allow_add['n_trades'] - no_add['n_trades']:>+10}"
    )
    print(
        f"{'익절수':<12}  {no_add['n_wins']:>10}  {allow_add['n_wins']:>10}"
        f"  {allow_add['n_wins'] - no_add['n_wins']:>+10}"
    )
    print(
        f"{'손절수':<12}  {no_add['n_losses']:>10}  {allow_add['n_losses']:>10}"
        f"  {allow_add['n_losses'] - no_add['n_losses']:>+10}"
    )
