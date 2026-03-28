"""
scanner_codex 전용 백테스트 시뮬레이터

- 과거 base_date 기준으로 scanner_codex 후보를 산출
- 다음 영업일(entry_date)부터 실제 일봉 OHLCV로 체결/청산 시뮬레이션
- setup(BREAKOUT / IGNITION / PULLBACK) 별 보유기간 차등 적용
- 주문 파일/DB 저장 없이 리포트 및 CSV 저장만 수행
"""

from __future__ import annotations

import logging
import sys
from dataclasses import dataclass
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

import pandas as pd

from wye.blsh.common.env import DATA_DIR, LOG_DIR, TRADE_FLAG
from wye.blsh.database import query
from wye.blsh.domestic import Tick
from wye.blsh.domestic.scanner_codex import find_candidates

log = logging.getLogger(__name__)
_fh = TimedRotatingFileHandler(
    LOG_DIR / "simulator_codex.log",
    when="midnight",
    backupCount=30,
    encoding="utf-8",
)
_fh.suffix = "%Y-%m-%d"
_fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
log.addHandler(_fh)

REPORT_DIR = DATA_DIR / "reports"
SELL_COST_RATE = 0.002


@dataclass(frozen=True)
class SimConfig:
    label: str
    hold_days: dict[str, int]
    trail_atr_mult: float


SIM_CONFIGS = {
    "DAY": SimConfig(
        label="DAY",
        hold_days={"BREAKOUT": 1, "IGNITION": 1, "PULLBACK": 2},
        trail_atr_mult=1.2,
    ),
    "SWING": SimConfig(
        label="SWING",
        hold_days={"BREAKOUT": 5, "IGNITION": 4, "PULLBACK": 7},
        trail_atr_mult=1.8,
    ),
}
SIM_CFG = SIM_CONFIGS.get(TRADE_FLAG, SIM_CONFIGS["SWING"])


def _fetch_ohlcv_index(entry_date: str, tickers: list[str], max_hold_days: int) -> dict[str, dict[str, dict]]:
    date_rows = pd.DataFrame(query.get_max_hold_dates(entry_date, max(max_hold_days, 1)))
    hold_dates = [entry_date] + (date_rows["d"].tolist() if not date_rows.empty else [])

    def fetch(table: str) -> pd.DataFrame:
        try:
            return pd.DataFrame(query.get_ohlcv_range(table, hold_dates, tickers))
        except Exception as exc:
            log.warning("OHLCV 조회 오류 (%s): %s", table, exc)
            return pd.DataFrame()

    all_rows = pd.concat(
        [fetch("isu_ksp_ohlcv"), fetch("isu_ksd_ohlcv")],
        ignore_index=True,
    )

    ohlcv_idx: dict[str, dict[str, dict]] = {}
    for _, row in all_rows.iterrows():
        ohlcv_idx.setdefault(row["ticker"], {})[row["trd_dd"]] = {
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
        }
    return ohlcv_idx


def _hold_days_for(setup: str) -> int:
    return SIM_CFG.hold_days.get(setup, max(SIM_CFG.hold_days.values()))


def _iter_trade_dates(entry_date: str, hold_days: int) -> list[str]:
    if hold_days <= 0:
        return [entry_date]
    rows = query.get_max_hold_dates(entry_date, hold_days)
    return [entry_date] + [r["d"] for r in rows]


def _simulate_one(sig: pd.Series, days: dict[str, dict]) -> dict:
    entry_date = str(sig["entry_date"])
    ticker = str(sig["ticker"])
    hold_days = _hold_days_for(str(sig["setup"]))
    trade_dates = _iter_trade_dates(entry_date, hold_days)

    first_day = days.get(entry_date)
    if first_day is None:
        return {**sig.to_dict(), "status": "MISS"}

    entry_limit = float(sig["entry_price"])
    if first_day["open"] > entry_limit:
        return {
            **sig.to_dict(),
            "status": "GAP",
            "result_type": "갭상승스킵",
            "buy_price": None,
            "exit_price": None,
            "exit_date": None,
            "ret_pct": None,
            "pnl_amount": None,
            "hold_days_used": 0,
            "t_open": first_day["open"],
        }

    buy_price = float(first_day["open"])
    atr = float(sig["atr"])
    stop_loss = Tick.floor_tick(buy_price - atr * SIM_CFG.trail_atr_mult)
    stop_loss = max(1, stop_loss)
    initial_stop = min(stop_loss, int(sig["stop_loss"]))
    stop_loss = initial_stop
    take_profit = int(sig["take_profit"])
    result_type = None
    exit_price = None
    exit_date = None
    last_ohv = first_day
    prev_high = first_day["high"]
    days_traded = 0

    for d in trade_dates:
        ohv = days.get(d)
        if ohv is None:
            continue
        days_traded += 1
        last_ohv = ohv

        if d != entry_date:
            trail_sl = Tick.floor_tick(prev_high - atr * SIM_CFG.trail_atr_mult)
            if trail_sl > stop_loss and trail_sl < prev_high:
                stop_loss = trail_sl

        if ohv["low"] <= stop_loss:
            result_type = "손절"
            exit_price = float(stop_loss)
            exit_date = d
            break

        if ohv["high"] >= take_profit:
            result_type = "익절"
            exit_price = float(take_profit)
            exit_date = d
            break

        prev_high = max(prev_high, ohv["high"])

    if result_type is None:
        result_type = "기간청산"
        exit_price = float(last_ohv["close"])
        exit_date = trade_dates[-1]

    pnl_amount = (exit_price - buy_price) - (exit_price * SELL_COST_RATE)
    ret_pct = (pnl_amount / buy_price) * 100
    return {
        **sig.to_dict(),
        "status": "OK",
        "result_type": result_type,
        "buy_price": round(buy_price, 2),
        "exit_price": round(exit_price, 2),
        "exit_date": exit_date,
        "ret_pct": round(ret_pct, 2),
        "pnl_amount": round(pnl_amount, 2),
        "hold_days_used": days_traded,
        "sim_max_hold_days": hold_days,
        "t_open": first_day["open"],
        "t_high": last_ohv["high"],
        "t_low": last_ohv["low"],
        "t_close": last_ohv["close"],
        "final_stop_loss": stop_loss,
        "ticker_sim": ticker,
    }


def simulate_candidates(candidates: pd.DataFrame, report: bool = True) -> pd.DataFrame:
    if candidates.empty:
        if report:
            print_simulation_report(candidates, pd.DataFrame())
        return pd.DataFrame()

    entry_date = str(candidates.iloc[0]["entry_date"])
    if not query.has_ohlcv_data(entry_date):
        log.warning("%s - entry_date ohlcv 데이터가 없습니다", entry_date)
        return pd.DataFrame()

    max_hold = max(_hold_days_for(str(s)) for s in candidates["setup"].unique())
    ohlcv_idx = _fetch_ohlcv_index(entry_date, candidates["ticker"].tolist(), max_hold)

    rows = []
    for _, sig in candidates.iterrows():
        rows.append(_simulate_one(sig, ohlcv_idx.get(sig["ticker"], {})))

    df = pd.DataFrame(rows)
    if not df.empty:
        order_cols = [
            "status",
            "ret_pct",
            "buy_score",
            "rr",
            "avg_trdval20",
        ]
        asc = [True, False, False, False, False]
        df = df.sort_values(order_cols, ascending=asc, na_position="last").reset_index(drop=True)

    if report:
        print_simulation_report(candidates, df)
    return df


def print_simulation_report(candidates: pd.DataFrame, results: pd.DataFrame):
    print()
    print("=" * 120)
    if candidates.empty:
        print("  Codex Simulation Report  |  후보 없음")
        print("=" * 120)
        return

    base_date = candidates.iloc[0]["base_date"]
    entry_date = candidates.iloc[0]["entry_date"]
    print(
        f"  Codex Simulation Report  |  strategy={SIM_CFG.label}"
        f"  base_date={base_date}  entry_date={entry_date}"
    )
    print("=" * 120)

    if results.empty:
        print("  시뮬레이션 결과 없음")
        print("=" * 120)
        return

    ok_df = results[results["status"] == "OK"].copy()
    gap_df = results[results["status"] == "GAP"].copy()
    miss_df = results[results["status"] == "MISS"].copy()

    avg_ret = ok_df["ret_pct"].mean() if not ok_df.empty else 0.0
    win_rate = (
        (ok_df["ret_pct"] > 0).mean() * 100
        if not ok_df.empty
        else 0.0
    )
    total_buy_amount = ok_df["buy_price"].sum() if not ok_df.empty else 0.0
    total_pnl_amount = ok_df["pnl_amount"].sum() if not ok_df.empty else 0.0
    total_ret_pct = (total_pnl_amount / total_buy_amount * 100) if total_buy_amount else 0.0
    print(
        f"  candidates={len(candidates)}"
        f"  executed={len(ok_df)}"
        f"  gap_skip={len(gap_df)}"
        f"  miss={len(miss_df)}"
        f"  total_pnl={total_pnl_amount:+,.0f}"
        f"  total_ret={total_ret_pct:+.2f}%"
        f"  avg_ret={avg_ret:+.2f}%"
        f"  win_rate={win_rate:.1f}%"
    )
    print("-" * 120)
    print(
        f"  총매수금액={total_buy_amount:,.0f}"
        f"  총손익금액={total_pnl_amount:+,.0f}"
        f"  전체수익률={total_ret_pct:+.2f}%"
    )
    print("-" * 120)

    if not ok_df.empty:
        summary = (
            ok_df.groupby(["market", "setup", "result_type"])
            .agg(
                종목수=("ticker", "count"),
                평균수익률=("ret_pct", "mean"),
                손익금액합계=("pnl_amount", "sum"),
                평균보유일=("hold_days_used", "mean"),
            )
            .round(2)
            .reset_index()
        )
        print(summary.to_string(index=False))
        print("-" * 120)

        for _, row in ok_df.iterrows():
            print(
                f"  [{row['result_type']:<6s}]"
                f" {row['ticker']} {row['name'][:14]:<14s}"
                f" {row['market']:<6s}"
                f" {row['setup']:<8s}"
                f" 매수 {row['buy_price']:>8,.0f}"
                f" 청산 {row['exit_price']:>8,.0f}"
                f" 손익 {row['pnl_amount']:>+8,.0f}"
                f" 수익률 {row['ret_pct']:>+6.2f}%"
                f" 보유 {int(row['hold_days_used'])}일"
                f" 점수 {int(row['buy_score']):>2d}"
            )
        print()

    if not gap_df.empty:
        print("  [갭상승 스킵]")
        for _, row in gap_df.iterrows():
            print(
                f"  {row['ticker']} {row['name'][:14]:<14s}"
                f" {row['market']:<6s}"
                f" {row['setup']:<8s}"
                f" 진입한도 {row['entry_price']:>8,.0f}"
                f" 시가 {row['t_open']:>8,.0f}"
            )
        print()

    if not miss_df.empty:
        print("  [데이터 누락]")
        for _, row in miss_df.iterrows():
            print(
                f"  {row['ticker']} {row['name'][:14]:<14s}"
                f" {row['market']:<6s}"
                f" {row['setup']:<8s}"
            )
        print()

    print("=" * 120)


def save_simulation(results: pd.DataFrame, base_date: str) -> Path:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    path = REPORT_DIR / f"simulator_codex_{SIM_CFG.label.lower()}_{base_date}.csv"
    results.to_csv(path, index=False, encoding="utf-8-sig")
    log.info("시뮬레이션 저장: %s", path)
    return path


def run(base_date: str, scan_report: bool = False, sim_report: bool = True, save_report: bool = False) -> tuple[pd.DataFrame, pd.DataFrame]:
    candidates = find_candidates(base_date=base_date, report=scan_report, save_report=False)
    results = simulate_candidates(candidates, report=sim_report)
    if save_report and not results.empty:
        save_simulation(results, base_date)
    return candidates, results


def main(argv: list[str] | None = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    if not argv:
        print("usage: python -m wye.blsh.domestic.simulator_codex YYYYMMDD [--scan-report] [--save]")
        return 1

    base_date = argv[0]
    scan_report = "--scan-report" in argv
    save_report = "--save" in argv
    _, results = run(
        base_date=base_date,
        scan_report=scan_report,
        sim_report=True,
        save_report=save_report,
    )
    return 0 if results is not None else 1


if __name__ == "__main__":
    raise SystemExit(main())
