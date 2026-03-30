"""
쿼리
"""

import logging
from sqlalchemy import text, bindparam
from sqlalchemy.orm import Session
from wye.blsh.database import (
    engine,
    select_one,
    select_first,
    select_all,
    execute_batch,
)
from wye.blsh.common import dtutils

log = logging.getLogger(__name__)

_min_krx_holiday_date = None


def _get_min_krx_holiday_date():
    global _min_krx_holiday_date
    if _min_krx_holiday_date is None:
        _min_krx_holiday_date = select_one("select min(bass_dt) as d from krx_holiday")[
            "d"
        ]
    return _min_krx_holiday_date


_ALLOWED_TABLES = {
    "isu_ksp_ohlcv",
    "isu_ksd_ohlcv",
    "isu_ksp_info",
    "isu_ksd_info",
    "idx_stk_ohlcv",
    "etf_ohlcv",
}


def _validate_table(table: str) -> None:
    if table not in _ALLOWED_TABLES:
        raise ValueError(f"허용되지 않은 테이블명: {table}")


def has_ohlcv_data(base_date) -> bool:
    return (
        select_first(
            "select trd_dd from idx_stk_ohlcv where trd_dd = :bd", bd=base_date
        )
        is not None
    )


def get_max_ohlcv_date():
    return select_one("select max(trd_dd) as d from idx_stk_ohlcv")["d"]


def get_fetched_at(base_date):
    return select_one(
        """
        select max(fetched_at) as t from idx_stk_ohlcv 
        where trd_dd=:bd
        """,
        bd=base_date,
    )["t"]


def find_next_biz_date(base_date) -> str | None:
    """다음 영업일"""
    if base_date < _get_min_krx_holiday_date():
        row = select_one(
            """
            SELECT min(trd_dd) AS d FROM idx_stk_ohlcv
            WHERE trd_dd > :bd
            """,
            bd=base_date,
        )
    else:
        row = select_first(
            """
            SELECT bass_dt AS d FROM krx_holiday
            WHERE bass_dt > :bd AND opnd_yn = 'Y'
            ORDER BY bass_dt
            LIMIT 1
            """,
            bd=base_date,
        )
    return row["d"] if row else None


def find_prev_biz_date(base_date) -> str | None:
    """이전 영업일"""
    if base_date <= _get_min_krx_holiday_date():
        row = select_one(
            """
            SELECT max(trd_dd) AS d FROM idx_stk_ohlcv
            WHERE trd_dd < :bd
            """,
            bd=base_date,
        )
    else:
        row = select_first(
            """
            SELECT bass_dt AS d FROM krx_holiday
            WHERE bass_dt < :bd AND opnd_yn = 'Y'
            ORDER BY bass_dt DESC
            LIMIT 1
            """,
            bd=base_date,
        )
    return row["d"] if row else None


def get_biz_dates(fromdate, todate):
    return select_all(
        """
        SELECT distinct trd_dd as d 
        FROM idx_stk_ohlcv 
        WHERE trd_dd >= :fd AND  trd_dd <= :td 
        UNION
        SELECT bass_dt AS d 
        FROM krx_holiday
        WHERE bass_dt >= :fd AND  bass_dt <= :td
        AND opnd_yn = 'Y'
        ORDER BY 1
        """,
        fd=fromdate,
        td=todate,
    )


def get_krx_holiday(base_date):
    return select_first(
        """
        SELECT * FROM krx_holiday
        WHERE bass_dt = :bd 
        """,
        bd=base_date,
    )


def get_krx_holiday_max_dt():
    return select_one(
        """
        SELECT max(bass_dt) as d FROM krx_holiday
        """,
    )["d"]


def save_holiday(df):
    """krx_holiday 테이블에 upsert."""
    if df is None or df.empty:
        return

    execute_batch(
        """
                INSERT INTO krx_holiday
                    (bass_dt, wday_dvsn_cd, bzdy_yn, opnd_yn, tr_day_yn, sttl_day_yn)
                VALUES
                    (%(bass_dt)s, %(wday_dvsn_cd)s, %(bzdy_yn)s, %(opnd_yn)s, %(tr_day_yn)s, %(sttl_day_yn)s)
                ON CONFLICT (bass_dt) DO NOTHING
            """,
        df.to_dict("records"),
    )


def get_netbid_trdvol(table, tickers, base_date):
    _validate_table(table)
    with Session(engine) as session:
        stmt = text(
            f"""
            SELECT isu_srt_cd, trd_dd,
                   frgn_netbid_trdvol AS frgn_qty,
                   inst_netbid_trdvol AS inst_qty,
                   indi_netbid_trdvol AS indi_qty
            FROM {table}
            WHERE isu_srt_cd IN :tickers
              AND trd_dd <= :bd
            ORDER BY isu_srt_cd, trd_dd DESC
            """
        ).bindparams(bindparam("tickers", expanding=True))
        result = (
            session.execute(stmt, {"tickers": list(tickers), "bd": base_date})
            .mappings()
            .all()
        )
    return result


def get_index_clsprc(idx_nm, base_date, ma_days=20, idx_clss=None):
    if idx_clss:
        return select_all(
            """
            SELECT clsprc_idx
            FROM idx_stk_ohlcv
            WHERE idx_nm = :nm AND idx_clss = :clss AND trd_dd <= :bd
            ORDER BY trd_dd DESC
            LIMIT :days
            """,
            **{"nm": idx_nm, "clss": idx_clss, "bd": base_date, "days": ma_days + 1},
        )
    return select_all(
        """
        SELECT clsprc_idx
        FROM idx_stk_ohlcv
        WHERE idx_nm = :nm AND trd_dd <= :bd
        ORDER BY trd_dd DESC
        LIMIT :days
        """,
        **{"nm": idx_nm, "bd": base_date, "days": ma_days + 1},
    )


def get_ohlcv(
    table, close_col, high_col, low_col, vol_col, params: dict, open_col=None
):
    _validate_table(table)
    open_select = f", o.{open_col}" if open_col else ""
    return select_all(
        f"""
                SELECT o.isu_srt_cd, o.trd_dd,
                    o.{close_col}, o.{high_col}, o.{low_col},
                    o.{vol_col},  o.acc_trdval{open_select}
                FROM {table} o
                WHERE o.trd_dd >= :start
                AND o.trd_dd <= :base_date
                AND o.isu_srt_cd IN (
                    SELECT isu_srt_cd
                    FROM {table}
                    WHERE trd_dd > :filter_start
                        AND trd_dd <= :base_date
                    GROUP BY isu_srt_cd
                    HAVING AVG(acc_trdval) >= :min_val
                )
                ORDER BY o.isu_srt_cd, o.trd_dd
            """,
        **params,
    )


def get_ohlcv_range(table, dates: list, tickers: list):
    _validate_table(table)
    with Session(engine) as session:
        stmt = text(
            f"""
            SELECT isu_srt_cd AS ticker,
                   trd_dd,
                   tdd_opnprc AS open,
                   tdd_hgprc  AS high,
                   tdd_lwprc  AS low,
                   tdd_clsprc AS close
            FROM {table}
            WHERE trd_dd IN :dates
              AND isu_srt_cd IN :tickers
            ORDER BY isu_srt_cd, trd_dd
            """
        ).bindparams(
            bindparam("dates", expanding=True),
            bindparam("tickers", expanding=True),
        )
        result = (
            session.execute(stmt, {"dates": dates, "tickers": tickers}).mappings().all()
        )
    return result


def get_ticker_name_map():
    result = select_all("SELECT isu_srt_cd, isu_abbrv FROM isu_base_info")
    return {row["isu_srt_cd"]: row["isu_abbrv"] for row in result}


def get_max_hold_dates(target_date, max_hold_days):
    return select_all(
        """
        SELECT DISTINCT bass_dt as d
        FROM krx_holiday
        WHERE bass_dt > :start
        AND opnd_yn='Y'
        ORDER BY bass_dt
        LIMIT :n
        """,
        **{"start": target_date, "n": max_hold_days},
    )


# ─────────────────────────────────────────
# 매매 이력
# ─────────────────────────────────────────
def save_trade_history(
    side: str,
    ticker: str,
    name: str,
    qty: int,
    price: float | None,
    reason: str = "",
    po_type: str = "",
):
    """매매 이력 1건 INSERT. ~1-5ms (동기, 스레드 불필요)."""
    from wye.blsh.common.env import KIS_ENV
    with Session(engine) as session:
        session.execute(
            text(
                """
                INSERT INTO trade_history (side, ticker, name, qty, price, reason, po_type, kis_env)
                VALUES (:side, :ticker, :name, :qty, :price, :reason, :po_type, :kis_env)
                """
            ),
            {
                "side": side,
                "ticker": ticker,
                "name": name,
                "qty": qty,
                "price": price,
                "reason": reason,
                "po_type": po_type or None,
                "kis_env": KIS_ENV,
            },
        )
        session.commit()


def get_trade_history(date_str: str | None = None):
    """당일 매매 이력 조회. date_str: YYYYMMDD (미지정 시 오늘). KIS_ENV 자동 필터."""
    from wye.blsh.common.env import KIS_ENV
    date_str = date_str or dtutils.today()
    return select_all(
        """
        SELECT * FROM trade_history
        WHERE traded_at::date = to_date(:d, 'YYYYMMDD')
          AND (kis_env = :env OR kis_env IS NULL)
        ORDER BY traded_at
        """,
        d=date_str,
        env=KIS_ENV,
    )


def get_latest_buy_history(tickers: list[str]) -> dict[str, dict]:
    """종목별 최근 60일 내 가장 최근 매수 이력 1건씩.

    positions.json 유실 시 복원에 사용.
    traded_at 날짜를 buy_date로 반환 (실제 체결일 — entry_date와 다를 수 있음).
    Returns: {ticker: {name, price, qty, buy_date, po_type}}
    """
    if not tickers:
        return {}
    from wye.blsh.common.env import KIS_ENV
    with Session(engine) as session:
        stmt = text(
            """
            SELECT DISTINCT ON (ticker)
                   ticker, name, price, qty, po_type,
                   to_char(traded_at, 'YYYYMMDD') AS buy_date
            FROM trade_history
            WHERE ticker IN :tickers
              AND side = 'buy'
              AND traded_at >= now() - INTERVAL '60 days'
              AND (kis_env = :env OR kis_env IS NULL)
            ORDER BY ticker, traded_at DESC
            """
        ).bindparams(bindparam("tickers", expanding=True))
        rows = session.execute(stmt, {"tickers": list(tickers), "env": KIS_ENV}).mappings().all()
    return {r["ticker"]: dict(r) for r in rows}


def get_today_sell_history(tickers: list[str], today: str) -> dict[str, dict]:
    """당일 종목별 최근 매도 이력 1건씩. t1_done 판단에 사용.

    Returns: {ticker: {reason, price, qty}}
    """
    if not tickers:
        return {}
    from wye.blsh.common.env import KIS_ENV
    with Session(engine) as session:
        stmt = text(
            """
            SELECT DISTINCT ON (ticker) ticker, reason, price, qty
            FROM trade_history
            WHERE ticker IN :tickers
              AND side = 'sell'
              AND traded_at::date = to_date(:d, 'YYYYMMDD')
              AND (kis_env = :env OR kis_env IS NULL)
            ORDER BY ticker, traded_at DESC
            """
        ).bindparams(bindparam("tickers", expanding=True))
        rows = session.execute(stmt, {"tickers": list(tickers), "d": today, "env": KIS_ENV}).mappings().all()
    return {r["ticker"]: dict(r) for r in rows}


def get_recent_ohlcv_for_atr(ticker: str, n: int = 25) -> list[dict]:
    """ATR 계산용 최근 OHLCV (고가/저가/종가). KOSPI 우선, 없으면 KOSDAQ.

    Returns: [{high, low, close}, ...] 오름차순 (오래된 것 먼저)
    """
    for table in ("isu_ksp_ohlcv", "isu_ksd_ohlcv"):
        try:
            rows = select_all(
                f"SELECT tdd_hgprc AS high, tdd_lwprc AS low, tdd_clsprc AS close "
                f"FROM {table} WHERE isu_srt_cd = :t ORDER BY trd_dd DESC LIMIT :n",
                t=ticker,
                n=n,
            )
            if rows:
                return list(reversed(rows))
        except Exception:
            continue
    return []


if __name__ == "__main__":
    print(get_max_hold_dates("20260312", 5))
