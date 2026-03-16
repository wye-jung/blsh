import logging
import time
from sqlalchemy import text, bindparam
from sqlalchemy.orm import Session
from blsh.database import engine, select_one, select_first, select_all, execute_batch

log = logging.getLogger(__name__)

_min_krx_holiday_date = select_one("select min(bass_dt) as d from krx_holiday")["d"]

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


def get_max_ohlcv_date():
    return select_one("select max(trd_dd) As d from idx_stk_ohlcv")["d"]


def get_latest_biz_date(base_date: str = time.strftime("%Y%m%d")) -> str:
    """최근 거래일"""
    row = select_one(
        """
        SELECT MAX(trd_dd) AS d FROM idx_stk_ohlcv 
        WHERE trd_dd <= :bd
        """,
        bd=base_date,
    )
    return row["d"] if row else None


def find_next_biz_date(base_date) -> str | None:
    """다음 영업일"""
    if base_date < _min_krx_holiday_date:
        row = select_first(
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
    )


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


def get_candidates(entry_date):
    return select_all(
        """
        SELECT *
        FROM trade_candidates
        WHERE entry_date = :td
        """,
        td=entry_date,
    )


def get_index_clsprc(idx_nm, base_date, ma_days=20):
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


def get_ohlcv(table, close_col, high_col, low_col, vol_col, params: dict):
    _validate_table(table)
    return select_all(
        f"""
                SELECT o.isu_srt_cd, o.trd_dd,
                    o.{close_col}, o.{high_col}, o.{low_col},
                    o.{vol_col},  o.acc_trdval
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


if __name__ == "__main__":
    print(get_max_hold_dates("20260312", 5))
