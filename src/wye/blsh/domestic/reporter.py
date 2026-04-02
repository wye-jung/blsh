"""
결과 리포팅 및 출력 모듈
"""

import logging
import pandas as pd
import numpy as np
from wye.blsh.domestic.config import INVEST_MIN_SCORE, MAX_HOLD_DAYS

log = logging.getLogger(__name__)


def print_general_summary(df):
    _print_header("스캔 리포트")

    if df.empty:
        log.info("─── 스캔 결과 없음 ───")
        return

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
    log.info("─── 시장별 요약 ───")
    print(summary.to_string(index=False))


def print_invest_report(df):
    _print_header("★ 투자 대상 선별 리포트")

    if df.empty:
        log.info("─── 투자 대상 없음 ───")
        return
    else:
        log.info(f"entry_date: {df.iloc[0]['entry_date']}  |  총 {len(df)}종목")

    candidates = df.copy()
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

    log.info(
        f"  선별 기준: score≥{INVEST_MIN_SCORE}  mode=MIX/MOM  수급(외인or기관)>0  P_OV 제외"
    )

    _line("=")

    if candidates.empty:
        log.info("  해당 조건을 만족하는 종목이 없습니다.")
        _line("=")
        return

    for mode_label, mode_val in [
        ("MIX (추세전환 초입 ★★★)", "MIX"),
        ("MOM (모멘텀 추종  ★★ )", "MOM"),
        ("REV (추세전환  ★  )", "REV"),
    ]:
        group = candidates[candidates["mode"] == mode_val]
        if group.empty:
            continue
        log.info(f"  【 {mode_label} 】  {len(group)}종목")
        _line("-", prefix="  ")

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
        log.info("")

    _line("=")

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
    entry_date, candidates, rows_ok, rows_gap, rows_miss, cash: float = 0
):
    _print_header(f"📊 수익률 리포트")

    cash_mode = cash > 0
    log.info(f"  ▶ 기간: 매수일({entry_date}) 이후 최대 {MAX_HOLD_DAYS}거래일")
    log.info(f"  ▶ 종목: {len(candidates)}건 (매수성공: {len(rows_ok)})")

    if rows_ok:
        df_ok = pd.DataFrame(rows_ok).sort_values("ret_pct", ascending=False)
        is_confirmed_win = df_ok["result_type"].str.startswith("익절")
        is_confirmed_cut = df_ok["result_type"] == "손절"
        is_hold = ~(is_confirmed_win | is_confirmed_cut)

        wins = df_ok[is_confirmed_win]

        confirmed_wins = df_ok[is_confirmed_win]
        confirmed_cuts = df_ok[is_confirmed_cut]
        hold_wins = df_ok[is_hold & (df_ok["ret_pct"] > 0)]
        hold_cuts = df_ok[is_hold & (df_ok["ret_pct"] < 0)]

        all_holds = df_ok[is_hold]
        avg_ret = df_ok["ret_pct"].mean()
        win_rate = len(wins) / len(df_ok) * 100 if len(df_ok) else 0
        log.info(
            f"  ▶ 평균 수익률: {avg_ret:+.2f}%  ▶ 승률: {win_rate:.1f}% "
            f"(익절 {len(confirmed_wins)} / 손절 {len(confirmed_cuts)} / "
            f"미확정 {len(all_holds)}(수익 {len(hold_wins)}, 손실 {len(hold_cuts)}))"
        )
        if cash_mode and df_ok["pnl_amount"].notna().any():
            total_pnl = df_ok["pnl_amount"].sum()
            total_invested = (df_ok["buy_price"] * df_ok["qty"]).sum()
            final_balance = cash - total_invested + total_invested + total_pnl
            log.info(
                f"  ▶ 초기잔고: {cash:>12,.0f}원  ▶ 총투자금: {total_invested:>12,.0f}원  ▶ 총손익: {total_pnl:>+12,.0f}원"
            )
            log.info(
                f"  ▶ 최종잔고: {final_balance:>12,.0f}원 ({final_balance / cash * 100:.1f}%)"
            )

        _line("─", prefix="  ")

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

            amt_str = ""
            if cash_mode and r.get("qty") and r.get("pnl_amount") is not None:
                invested = r["buy_price"] * r["qty"]
                amt_str = (
                    f"  {r['qty']}주 (투자 {invested:>9,.0f}원)"
                    f"  손익 {r['pnl_amount']:>+10,.0f}원"
                )

            log.info(
                f"  {tag:<10s}  [{r['buy_score']:2d}pt/{r['mode']}]  "
                f"{r['ticker']}  {r['name'][:12]:<12s}  {r['market']:<6s}  "
                f"매수 {r['buy_price']:>8,.0f} ({r['entry_date']})  "
                f"청산 {r['exit_price']:>8,.0f} ({r['exit_date']})  "
                f"수익률 {r['ret_pct']:>+6.2f}%{amt_str}"
            )

        _print_loss_stats(df_ok, wins, confirmed_cuts, hold_cuts)

    if rows_gap:
        log.info(f"  ▶ 갭 상승 (매수 불가) {len(rows_gap)}종목")
        _line("-", prefix="  ")
        for r in rows_gap:
            log.info(
                f"  ⬆️갭상승  [{r['buy_score']:2d}pt/{r['mode']}]  "
                f"{r['ticker']}  {r['name'][:12]:<12s}  "
                f"진입가 {r['entry_price']:>8,.0f}  "
                f"시가 {r['t_open']:>8,.0f} ({r['entry_date']})"
            )

    _line()


_SUPPLY_FLAGS = {"F_TRN", "I_TRN", "F_C3", "I_C3", "F_1", "I_1"}


def _print_loss_stats(df_ok, wins, confirmed_cuts, hold_cuts):
    losers = pd.concat([confirmed_cuts, hold_cuts], ignore_index=True)
    if losers.empty:
        return

    _line("─", prefix="  ")
    log.info(
        f"  [ 손실 종목 분석 ]  손절 {len(confirmed_cuts)} + 미확정손실 {len(hold_cuts)} = {len(losers)}건"
    )
    _line("─", prefix="  ")

    # mode 분포 비교
    def mode_dist(df):
        vc = df["mode"].value_counts()
        total = len(df)
        return (
            "  ".join(f"{k} {v}건({v / total * 100:.0f}%)" for k, v in vc.items())
            if total
            else "-"
        )

    log.info(f"  mode | 손실: {mode_dist(losers)}")
    log.info(f"       | 승리: {mode_dist(wins)}")

    # flag 출현 빈도 비교 (상위 10)
    def flag_counts(df):
        counts: dict[str, int] = {}
        for flags_str in df["buy_flags"].dropna():
            for f in str(flags_str).split():
                counts[f] = counts.get(f, 0) + 1
        total = len(df) or 1
        return {
            k: (v, v / total * 100)
            for k, v in sorted(counts.items(), key=lambda x: -x[1])
        }

    loss_fc = flag_counts(losers)
    win_fc = flag_counts(wins)
    all_flags = list(dict.fromkeys(list(loss_fc)[:10] + list(win_fc)[:10]))

    log.info(f"  {'flag':<8s}  {'손실':>10s}  {'승리':>10s}  차이")
    for f in all_flags:
        lv, lp = loss_fc.get(f, (0, 0.0))
        wv, wp = win_fc.get(f, (0, 0.0))
        diff = lp - wp
        marker = " ◀ 손실편향" if diff > 15 else (" ▶ 승리편향" if diff < -15 else "")
        log.info(
            f"  {f:<8s}  {lv:3d}건({lp:4.0f}%)  {wv:3d}건({wp:4.0f}%)  {diff:+.0f}%{marker}"
        )

    # 수급 flag 유무 비교
    def supply_rate(df):
        if df.empty:
            return 0.0
        has = (
            df["buy_flags"]
            .dropna()
            .apply(lambda s: any(f in str(s).split() for f in _SUPPLY_FLAGS))
        )
        return has.mean() * 100

    log.info(
        f"  수급보유 비율 | 손실: {supply_rate(losers):.0f}%  승리: {supply_rate(wins):.0f}%"
    )

    # 평균 점수 비교
    log.info(
        f"  평균 점수    | 손실: {losers['buy_score'].mean():.1f}점  "
        f"승리: {wins['buy_score'].mean():.1f}점"
    )


def _line(char="═", length=110, prefix="", appendix=""):
    log.info(prefix + char * length + appendix)


def _print_header(title):
    _line()
    log.info(f"  {title}")
    _line()
