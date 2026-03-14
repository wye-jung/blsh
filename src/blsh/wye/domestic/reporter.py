"""
결과 리포팅 및 출력 모듈
"""

import logging
import pandas as pd
import numpy as np
from blsh.wye.domestic import _factor as fac

log = logging.getLogger(__name__)


def print_general_summary(results):
    _print_header("스캔 리포트")

    if not results:
        return
    df = pd.DataFrame(results)

    summary = (
        df.groupby("market")
        .agg(
            종목수=("ticker", "count"),
            평균점수=("buy_score", "mean"),
            최고점수=("buy_score", "max"),
            강한신호=("buy_score", lambda x: (x >= 5).sum()),
            외국인순매수=(
                "foreign_netbuy",
                lambda x: (pd.to_numeric(x, errors="coerce") > 0).sum(),
            ),
            기관순매수=(
                "inst_netbuy",
                lambda x: (pd.to_numeric(x, errors="coerce") > 0).sum(),
            ),
        )
        .round(2)
        .reset_index()
    )
    log.info("\n─── 시장별 요약 ───\n" + summary.to_string(index=False))

    top = df.sort_values("buy_score", ascending=False).head(15)[
        [
            "ticker",
            "name",
            "market",
            "buy_score",
            "mode",
            "close",
            "entry_price",
            "stop_loss",
            "take_profit",
            "foreign_netbuy",
            "inst_netbuy",
            "indi_netbuy",
            "buy_flags",
        ]
    ]
    log.info("\n─── 매수 신호 TOP15 ───\n" + top.to_string(index=False))


def print_invest_report(results, base_date):
    _print_header("투자 대상 리포트")

    if not results:
        log.info("\n─── 투자 대상 없음 ───")
        return

    df = pd.DataFrame(results)
    for col in ("foreign_netbuy", "inst_netbuy", "indi_netbuy"):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    mask = (
        (df["buy_score"] >= fac.INVEST_MIN_SCORE)
        & (df["mode"].isin(["MIX", "MOM"]))
        & ((df["foreign_netbuy"] > 0) | (df["inst_netbuy"] > 0))
        & (~df["buy_flags"].str.contains("P_OV", na=False))
    )
    candidates = df[mask].copy()

    candidates["_mode_rank"] = candidates["mode"].map({"MIX": 0, "MOM": 1}).fillna(2)

    def supply_strength(flags: str) -> int:
        if not isinstance(flags, str):
            return 0
        if "TRN" in flags:
            return 3
        if "C3" in flags:
            return 2
        if "F_1" in flags or "I_1" in flags:
            return 1
        return 0

    candidates["_supply_rank"] = candidates["buy_flags"].apply(supply_strength)
    candidates = candidates.sort_values(
        ["_mode_rank", "_supply_rank", "buy_score"], ascending=[True, False, False]
    )

    sep = "═" * 110
    log.info(f"\n{sep}")
    log.info(
        f"  ★ 투자 대상 선별 리포트  |  기준일: {base_date}  |  총 {len(candidates)}종목"
    )
    log.info(
        f"  선별 기준: score≥{fac.INVEST_MIN_SCORE}  mode=MIX/MOM  수급(외인or기관)>0  P_OV 제외"
    )
    log.info(sep)

    if candidates.empty:
        log.info("  해당 조건을 만족하는 종목이 없습니다.")
        log.info(sep)
        return

    for mode_label, mode_val in [
        ("MIX (추세전환 초입 ★★★)", "MIX"),
        ("MOM (모멘텀 추종  ★★ )", "MOM"),
    ]:
        group = candidates[candidates["mode"] == mode_val]
        if group.empty:
            continue
        log.info(f"\n  【 {mode_label} 】  {len(group)}종목")
        log.info("  " + "─" * 108)

        for _, row in group.iterrows():
            frgn = (
                f"{row['foreign_netbuy']:+,.0f}"
                if pd.notna(row["foreign_netbuy"])
                else "N/A"
            )
            inst = (
                f"{row['inst_netbuy']:+,.0f}" if pd.notna(row["inst_netbuy"]) else "N/A"
            )
            indi = (
                f"{row['indi_netbuy']:+,.0f}" if pd.notna(row["indi_netbuy"]) else "N/A"
            )
            sl_gap = row["close"] - row["stop_loss"]
            rr = (
                (row["take_profit"] - row["close"]) / sl_gap
                if sl_gap > 0
                else float("nan")
            )
            rr_str = f"{rr:.1f}" if pd.notna(rr) else "N/A"
            log.info(
                f"  [{row['buy_score']:2d}pt] {row['ticker']}  {row['name'][:14]:<14s}  "
                f"{row['market']:<6s}  "
                f"종가 {row['close']:>8,.0f}  진입≤{row['entry_price']:>8,.0f}  "
                f"손절 {row['stop_loss']:>8,.0f}  익절 {row['take_profit']:>8,.0f}  "
                f"RR {rr_str}  "
                f"외인 {frgn:>12s}  기관 {inst:>12s}  개인 {indi:>12s}  "
                f"flags: {row['buy_flags']}"
            )

    log.info(f"\n{sep}\n")

    if not candidates.empty:
        log.info("  [ 선별 종목 분포 ]")
        log.info(
            "  mode별:  "
            + "  ".join(
                f"{k}={v}" for k, v in candidates["mode"].value_counts().items()
            )
        )
        log.info(
            "  시장별:  "
            + "  ".join(
                f"{k}={v}" for k, v in candidates["market"].value_counts().items()
            )
        )
        avg_rr = candidates.apply(
            lambda r: (
                (r["take_profit"] - r["close"]) / (r["close"] - r["stop_loss"])
                if (r["close"] - r["stop_loss"]) > 0
                else np.nan
            ),
            axis=1,
        ).mean()
        log.info(
            f"  평균 점수: {candidates['buy_score'].mean():.1f}점  평균 RR: {avg_rr:.2f}\n"
        )


def print_simul_report(
    base_date, target_date, actual_days, candidates, rows_ok, rows_gap, rows_miss
):
    _print_header("시뮬레이션 리포트")

    sep = "═" * 115
    log.info(f"\n{sep}")
    log.info(
        f"  📊 수익률 리포트  |  기준일: {base_date}  →  목표일: {target_date}"
        f"  (최대 {fac.MAX_HOLD_DAYS}거래일, 실제 {actual_days}거래일)"
    )
    log.info(
        f"  대상: 선별 종목 {len(candidates)}개  "
        f"/ 매수 성공: {len(rows_ok)}  갭 상승(매수 불가): {len(rows_gap)}  "
        f"데이터 없음: {len(rows_miss)}"
    )
    log.info(sep)

    if rows_ok:
        df_ok = pd.DataFrame(rows_ok).sort_values("ret_pct", ascending=False)
        wins = df_ok[df_ok["result_type"] == "익절"]
        cuts = df_ok[df_ok["result_type"] == "손절"]
        holds = df_ok[~df_ok["result_type"].isin(["익절", "손절"])]

        log.info(
            f"\n  ▶ 매수 성공 {len(df_ok)}종목  "
            f"(익절 {len(wins)}  손절 {len(cuts)}  미확정 {len(holds)})"
        )
        log.info("  " + "─" * 113)

        for _, r in df_ok.iterrows():
            if r["result_type"] == "익절":
                tag = "✅익절"
            elif r["result_type"] == "손절":
                tag = "❌손절"
            else:
                if r["ret_pct"] > 0:
                    tag = f"⏳수익({r['ret_pct']:+.2f}%)"
                elif r["ret_pct"] < 0:
                    tag = f"⏳손실({r['ret_pct']:+.2f}%)"
                else:
                    tag = f"⏳{r['result_type']}"

            log.info(
                f"  {tag:<10s}  [{r['buy_score']:2d}pt/{r['mode']}]  "
                f"{r['ticker']}  {r['name'][:12]:<12s}  {r['market']:<6s}  "
                f"매수 {r['buy_price']:>8,.0f} ({r['entry_date']})  "
                f"청산 {r['exit_price']:>8,.0f} ({r['exit_date']})  "
                f"수익률 {r['ret_pct']:>+6.2f}%"
            )

        avg_ret = df_ok["ret_pct"].mean()
        win_rate = len(wins) / len(df_ok) * 100 if len(df_ok) else 0
        log.info(
            f"\n  평균 수익률: {avg_ret:+.2f}%  승률: {win_rate:.1f}%  "
            f"(익절 {len(wins)} / 손절 {len(cuts)} / 미확정 {len(holds)})"
        )

    if rows_gap:
        log.info(f"\n  ▶ 갭 상승 (매수 불가) {len(rows_gap)}종목")
        log.info("  " + "─" * 113)
        for r in rows_gap:
            log.info(
                f"  ⬆️갭상승  [{r['buy_score']:2d}pt/{r['mode']}]  "
                f"{r['ticker']}  {r['name'][:12]:<12s}  "
                f"진입가 {r['entry_price']:>8,.0f}  "
                f"시가 {r['t_open']:>8,.0f} ({r['entry_date']})"
            )

    log.info(f"\n{sep}\n")


def _print_header(title):
    print("\n" + "#" * 150)
    log.info(title)
    print("#" * 150)
