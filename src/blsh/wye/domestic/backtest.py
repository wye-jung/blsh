"""
포트폴리오 백테스트: 2024년 최초 영업일 ~ 현재

초기 자본 1,000만원으로 매 영업일 scan → simulate를 실행하여
수익/손절/미확정 거래를 기록하고 포트폴리오 가치를 추적합니다.

실행:
    uv run python -m blsh.wye.domestic.backtest

저장 테이블:
    backtest_trades    - 개별 거래 기록
    backtest_daily     - 일별 포트폴리오 가치 및 수익률
    backtest_monthly   - 월별 집계
    backtest_quarterly - 분기별 집계
    backtest_yearly    - 연도별 집계

포트폴리오 전략:
    - 매 기준일(base_date)의 투자 대상 종목에 가용 현금 × 90% 를 균등 배분
    - simulate 결과(익절/손절/미확정)로 현금 회수 후 다음 배치로 이전
    - 배치 간 순서는 base_date 오름차순이며 동시 보유 포지션 가능
"""

import logging
import pandas as pd
from sqlalchemy import text, bindparam
from sqlalchemy.orm import Session

from blsh.database import engine, select_all, execute_batch
from blsh.wye.domestic import scanner, simulator, _factor as fac

log = logging.getLogger(__name__)

INITIAL_CAPITAL = 10_000_000
CASH_USAGE = 0.9       # 가용 현금의 90% 사용
MIN_ALLOC = 10_000     # 종목당 최소 배분액 (1만원)


# ─────────────────────────────────────────
# 메인
# ─────────────────────────────────────────
def run(initial_capital=INITIAL_CAPITAL, from_date="20240102"):
    """백테스트 실행 후 DB 저장"""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )

    _create_tables()

    biz_dates = _get_biz_dates(from_date)
    if not biz_dates:
        log.warning("[백테스트] 영업일 데이터 없음 (collector를 먼저 실행하세요)")
        return

    log.info(
        f"[백테스트] 시작  초기자본={initial_capital:,.0f}원  "
        f"기간={biz_dates[0]} ~ {biz_dates[-1]}  ({len(biz_dates)}일)"
    )

    # 포트폴리오 상태
    cash = float(initial_capital)
    # open_positions: [{base_date, ticker, allocated, ret_pct, entry_date, exit_date, ...}, ...]
    pending: list[dict] = []   # simulate 결과 대기 중 (target_date 아직 미래)
    all_trades: list[dict] = []
    daily_rows: list[dict] = []

    # 기준일별 scan + simulate → 거래 결과 수집
    # (target_date 기준으로 현금 흐름을 처리)
    batch_results: dict[str, list[dict]] = {}  # target_date → rows_ok 리스트

    for base_date in biz_dates:
        log.info(f"── 스캔: {base_date}")
        try:
            results, target_date, bd = scanner.scan(base_date)
        except Exception as e:
            log.warning(f"  scan 오류 ({base_date}): {e}")
            continue

        if not results or not target_date:
            continue

        screened = scanner.screen(results, bd)
        if not screened:
            continue

        try:
            ret = simulator.simulate(screened, target_date)
        except Exception as e:
            log.warning(f"  simulate 오류 ({base_date}): {e}")
            continue

        if ret is None:
            continue
        rows_ok, rows_gap, rows_miss = ret

        if rows_ok:
            batch_results.setdefault(target_date, []).extend(rows_ok)

    # ── 날짜 순으로 포트폴리오 추적
    # 각 target_date의 거래들을 해당 entry_date에 진입, exit_date에 회수
    prev_val = float(initial_capital)
    open_positions: list[dict] = []  # 아직 청산되지 않은 포지션

    # 모든 거래 flat하게 준비
    all_raw: list[dict] = []
    for target_date, rows in batch_results.items():
        for r in rows:
            all_raw.append(r)

    # entry_date별 그룹핑 (포지션 진입 처리용)
    entries_by_date: dict[str, list[dict]] = {}
    for r in all_raw:
        ed = r.get("entry_date") or r.get("target_date") or r.get("base_date")
        if ed:
            entries_by_date.setdefault(ed, []).append(r)

    for date in biz_dates:
        # 1. 오늘 진입 (이미 보유 중인 종목 제외)
        open_tickers = {p["ticker"] for p in open_positions}
        new_entries = [
            r for r in entries_by_date.get(date, [])
            if r["ticker"] not in open_tickers
        ]
        if new_entries:
            avail = cash * CASH_USAGE
            alloc = avail / len(new_entries) if new_entries else 0
            if alloc >= MIN_ALLOC:
                for r in new_entries:
                    cash -= alloc
                    open_positions.append({**r, "allocated": alloc})

        # 2. 오늘 청산 (exit_date == date)
        still_open = []
        closed_today = []
        for pos in open_positions:
            if pos.get("exit_date") == date:
                ret_pct = float(pos.get("ret_pct", 0))
                exit_val = pos["allocated"] * (1 + ret_pct / 100)
                cash += exit_val
                profit_amt = exit_val - pos["allocated"]
                result_type = pos.get("result_type", "미확정")
                all_trades.append({
                    "base_date":    pos["base_date"],
                    "target_date":  pos.get("target_date"),
                    "ticker":       pos["ticker"],
                    "name":         pos.get("name"),
                    "market":       pos.get("market"),
                    "mode":         pos.get("mode"),
                    "buy_score":    int(pos.get("buy_score", 0)),
                    "buy_flags":    pos.get("buy_flags"),
                    "entry_date":   pos.get("entry_date"),
                    "exit_date":    date,
                    "result_type":  result_type,
                    "buy_price":    pos.get("buy_price"),
                    "exit_price":   pos.get("exit_price"),
                    "ret_pct":      round(ret_pct, 4),
                    "allocated_amt": pos["allocated"],
                    "profit_amt":   round(profit_amt, 2),
                })
                closed_today.append(pos)
            else:
                still_open.append(pos)
        open_positions = still_open

        # 3. 당일 포트폴리오 평가
        # 미청산 포지션은 현재가 알 수 없으므로 취득원가로 평가 (보수적)
        unrealized = sum(p["allocated"] for p in open_positions)
        portfolio_val = cash + unrealized

        n_total = len(closed_today)
        n_wins = sum(1 for t in closed_today if t.get("result_type") == "익절")
        n_losses = sum(1 for t in closed_today if t.get("result_type") == "손절")
        n_hold_profit = sum(
            1 for t in closed_today if "수익" in str(t.get("result_type", ""))
        )
        n_hold_loss = sum(
            1 for t in closed_today if "손실" in str(t.get("result_type", ""))
        )
        decisive = n_wins + n_losses

        daily_rows.append({
            "trade_date":    date,
            "portfolio_val": round(portfolio_val, 2),
            "daily_ret_pct": round(
                (portfolio_val - prev_val) / prev_val * 100, 4
            ) if prev_val else 0,
            "cum_ret_pct": round(
                (portfolio_val - initial_capital) / initial_capital * 100, 4
            ),
            "profit_amt":    round(portfolio_val - prev_val, 2),
            "n_trades":      n_total,
            "n_wins":        n_wins,
            "n_losses":      n_losses,
            "n_hold_profit": n_hold_profit,
            "n_hold_loss":   n_hold_loss,
            "win_rate": round(n_wins / decisive * 100, 2) if decisive else None,
        })
        prev_val = portfolio_val

    # 미청산 포지션 → 마지막 날짜 기준 강제 기록 (취득가 기준)
    last_date = biz_dates[-1]
    for pos in open_positions:
        all_trades.append({
            "base_date":    pos["base_date"],
            "target_date":  pos.get("target_date"),
            "ticker":       pos["ticker"],
            "name":         pos.get("name"),
            "market":       pos.get("market"),
            "mode":         pos.get("mode"),
            "buy_score":    int(pos.get("buy_score", 0)),
            "buy_flags":    pos.get("buy_flags"),
            "entry_date":   pos.get("entry_date"),
            "exit_date":    last_date,
            "result_type":  "미청산",
            "buy_price":    pos.get("buy_price"),
            "exit_price":   pos.get("buy_price"),
            "ret_pct":      0.0,
            "allocated_amt": pos["allocated"],
            "profit_amt":   0.0,
        })

    # 저장
    _save_trades(all_trades)
    _save_daily(daily_rows)
    _save_period(daily_rows, all_trades, lambda d: d[:6], "backtest_monthly", "ym")
    _save_period(
        daily_rows, all_trades, _quarterly_key, "backtest_quarterly", "yq"
    )
    _save_period(daily_rows, all_trades, lambda d: d[:4], "backtest_yearly", "year")

    final_val = daily_rows[-1]["portfolio_val"] if daily_rows else initial_capital
    total_ret = (final_val - initial_capital) / initial_capital * 100
    log.info(
        f"[백테스트] 완료  총 거래: {len(all_trades)}건  "
        f"최종 포트폴리오: {final_val:,.0f}원  총 수익률: {total_ret:+.2f}%"
    )


# ─────────────────────────────────────────
# 헬퍼
# ─────────────────────────────────────────
def _get_biz_dates(from_date: str) -> list[str]:
    rows = select_all(
        "SELECT DISTINCT trd_dd FROM isu_ksp_ohlcv WHERE trd_dd >= :fd ORDER BY trd_dd",
        fd=from_date,
    )
    return [r["trd_dd"] for r in rows]


def _quarterly_key(date_str: str) -> str:
    q = (int(date_str[4:6]) - 1) // 3 + 1
    return f"{date_str[:4]}Q{q}"


# ─────────────────────────────────────────
# 저장
# ─────────────────────────────────────────
def _save_trades(trades: list[dict]):
    if not trades:
        return
    execute_batch(
        """
        INSERT INTO backtest_trades
            (base_date, target_date, ticker, name, market, mode, buy_score, buy_flags,
             entry_date, exit_date, result_type, buy_price, exit_price,
             ret_pct, allocated_amt, profit_amt)
        VALUES
            (%(base_date)s, %(target_date)s, %(ticker)s, %(name)s, %(market)s,
             %(mode)s, %(buy_score)s, %(buy_flags)s,
             %(entry_date)s, %(exit_date)s, %(result_type)s, %(buy_price)s, %(exit_price)s,
             %(ret_pct)s, %(allocated_amt)s, %(profit_amt)s)
        ON CONFLICT (base_date, ticker) DO UPDATE SET
            exit_date     = EXCLUDED.exit_date,
            result_type   = EXCLUDED.result_type,
            exit_price    = EXCLUDED.exit_price,
            ret_pct       = EXCLUDED.ret_pct,
            profit_amt    = EXCLUDED.profit_amt
        """,
        trades,
    )
    log.info(f"backtest_trades 저장: {len(trades)}건")


def _save_daily(daily_rows: list[dict]):
    if not daily_rows:
        return
    execute_batch(
        """
        INSERT INTO backtest_daily
            (trade_date, portfolio_val, daily_ret_pct, cum_ret_pct, profit_amt,
             n_trades, n_wins, n_losses, n_hold_profit, n_hold_loss, win_rate)
        VALUES
            (%(trade_date)s, %(portfolio_val)s, %(daily_ret_pct)s, %(cum_ret_pct)s,
             %(profit_amt)s, %(n_trades)s, %(n_wins)s, %(n_losses)s,
             %(n_hold_profit)s, %(n_hold_loss)s, %(win_rate)s)
        ON CONFLICT (trade_date) DO UPDATE SET
            portfolio_val  = EXCLUDED.portfolio_val,
            daily_ret_pct  = EXCLUDED.daily_ret_pct,
            cum_ret_pct    = EXCLUDED.cum_ret_pct,
            profit_amt     = EXCLUDED.profit_amt,
            n_trades       = EXCLUDED.n_trades,
            n_wins         = EXCLUDED.n_wins,
            n_losses       = EXCLUDED.n_losses,
            n_hold_profit  = EXCLUDED.n_hold_profit,
            n_hold_loss    = EXCLUDED.n_hold_loss,
            win_rate       = EXCLUDED.win_rate
        """,
        daily_rows,
    )
    log.info(f"backtest_daily 저장: {len(daily_rows)}건")


def _save_period(
    daily_rows: list[dict],
    all_trades: list[dict],
    key_fn,
    table: str,
    period_col: str,
):
    if not daily_rows:
        return

    df_daily = pd.DataFrame(daily_rows)
    df_trades = pd.DataFrame(all_trades) if all_trades else pd.DataFrame()
    df_daily["_period"] = df_daily["trade_date"].apply(key_fn)

    rows = []
    period_start_val = None

    for period, grp in df_daily.groupby("_period", sort=True):
        end_val = float(grp.iloc[-1]["portfolio_val"])

        if period_start_val is None:
            # 첫 기간 시작 포트폴리오: 첫날 이전 가치 (= 첫날 이전 daily_ret 역산)
            period_start_val = float(grp.iloc[0]["portfolio_val"]) - float(
                grp.iloc[0]["profit_amt"]
            )

        period_ret = (
            (end_val - period_start_val) / period_start_val * 100
            if period_start_val
            else 0
        )

        if not df_trades.empty and "exit_date" in df_trades.columns:
            pt = df_trades[df_trades["exit_date"].apply(key_fn) == period]
            n_trades = len(pt)
            n_wins = int((pt["result_type"] == "익절").sum())
            n_losses = int((pt["result_type"] == "손절").sum())
            n_hold_profit = int(
                pt["result_type"].fillna("").str.contains("수익").sum()
            )
            n_hold_loss = int(
                pt["result_type"].fillna("").str.contains("손실").sum()
            )
            decisive = n_wins + n_losses
        else:
            n_trades = n_wins = n_losses = n_hold_profit = n_hold_loss = decisive = 0

        rows.append({
            period_col:       period,
            "portfolio_val":  round(end_val, 2),
            "period_ret_pct": round(period_ret, 4),
            "cum_ret_pct":    float(grp.iloc[-1]["cum_ret_pct"]),
            "profit_amt":     round(float(grp["profit_amt"].sum()), 2),
            "n_trades":       n_trades,
            "n_wins":         n_wins,
            "n_losses":       n_losses,
            "n_hold_profit":  n_hold_profit,
            "n_hold_loss":    n_hold_loss,
            "win_rate":       round(n_wins / decisive * 100, 2) if decisive else None,
        })
        period_start_val = end_val

    execute_batch(
        f"""
        INSERT INTO {table}
            ({period_col}, portfolio_val, period_ret_pct, cum_ret_pct, profit_amt,
             n_trades, n_wins, n_losses, n_hold_profit, n_hold_loss, win_rate)
        VALUES
            (%({period_col})s, %(portfolio_val)s, %(period_ret_pct)s, %(cum_ret_pct)s,
             %(profit_amt)s, %(n_trades)s, %(n_wins)s, %(n_losses)s,
             %(n_hold_profit)s, %(n_hold_loss)s, %(win_rate)s)
        ON CONFLICT ({period_col}) DO UPDATE SET
            portfolio_val  = EXCLUDED.portfolio_val,
            period_ret_pct = EXCLUDED.period_ret_pct,
            cum_ret_pct    = EXCLUDED.cum_ret_pct,
            profit_amt     = EXCLUDED.profit_amt,
            n_trades       = EXCLUDED.n_trades,
            n_wins         = EXCLUDED.n_wins,
            n_losses       = EXCLUDED.n_losses,
            n_hold_profit  = EXCLUDED.n_hold_profit,
            n_hold_loss    = EXCLUDED.n_hold_loss,
            win_rate       = EXCLUDED.win_rate
        """,
        rows,
    )
    log.info(f"{table} 저장: {len(rows)}건")


# ─────────────────────────────────────────
# 테이블 생성
# ─────────────────────────────────────────
def _create_tables():
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS backtest_trades (
                base_date      VARCHAR(8)   NOT NULL,
                ticker         VARCHAR(10)  NOT NULL,
                target_date    VARCHAR(8),
                name           VARCHAR(60),
                market         VARCHAR(10),
                mode           VARCHAR(10),
                buy_score      INTEGER,
                buy_flags      TEXT,
                entry_date     VARCHAR(8),
                exit_date      VARCHAR(8),
                result_type    VARCHAR(20),
                buy_price      NUMERIC(15,2),
                exit_price     NUMERIC(15,2),
                ret_pct        NUMERIC(10,4),
                allocated_amt  NUMERIC(15,2),
                profit_amt     NUMERIC(15,2),
                created_at     TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (base_date, ticker)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS backtest_daily (
                trade_date     VARCHAR(8) PRIMARY KEY,
                portfolio_val  NUMERIC(15,2),
                daily_ret_pct  NUMERIC(10,4),
                cum_ret_pct    NUMERIC(10,4),
                profit_amt     NUMERIC(15,2),
                n_trades       INTEGER DEFAULT 0,
                n_wins         INTEGER DEFAULT 0,
                n_losses       INTEGER DEFAULT 0,
                n_hold_profit  INTEGER DEFAULT 0,
                n_hold_loss    INTEGER DEFAULT 0,
                win_rate       NUMERIC(6,2)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS backtest_monthly (
                ym             VARCHAR(6) PRIMARY KEY,
                portfolio_val  NUMERIC(15,2),
                period_ret_pct NUMERIC(10,4),
                cum_ret_pct    NUMERIC(10,4),
                profit_amt     NUMERIC(15,2),
                n_trades       INTEGER DEFAULT 0,
                n_wins         INTEGER DEFAULT 0,
                n_losses       INTEGER DEFAULT 0,
                n_hold_profit  INTEGER DEFAULT 0,
                n_hold_loss    INTEGER DEFAULT 0,
                win_rate       NUMERIC(6,2)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS backtest_quarterly (
                yq             VARCHAR(6) PRIMARY KEY,
                portfolio_val  NUMERIC(15,2),
                period_ret_pct NUMERIC(10,4),
                cum_ret_pct    NUMERIC(10,4),
                profit_amt     NUMERIC(15,2),
                n_trades       INTEGER DEFAULT 0,
                n_wins         INTEGER DEFAULT 0,
                n_losses       INTEGER DEFAULT 0,
                n_hold_profit  INTEGER DEFAULT 0,
                n_hold_loss    INTEGER DEFAULT 0,
                win_rate       NUMERIC(6,2)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS backtest_yearly (
                year           VARCHAR(4) PRIMARY KEY,
                portfolio_val  NUMERIC(15,2),
                period_ret_pct NUMERIC(10,4),
                cum_ret_pct    NUMERIC(10,4),
                profit_amt     NUMERIC(15,2),
                n_trades       INTEGER DEFAULT 0,
                n_wins         INTEGER DEFAULT 0,
                n_losses       INTEGER DEFAULT 0,
                n_hold_profit  INTEGER DEFAULT 0,
                n_hold_loss    INTEGER DEFAULT 0,
                win_rate       NUMERIC(6,2)
            )
        """))
        conn.commit()
    log.info("백테스트 테이블 생성 완료")


if __name__ == "__main__":
    run()
