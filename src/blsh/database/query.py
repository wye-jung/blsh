import logging
import time
from sqlalchemy import text, bindparam
from sqlalchemy.orm import Session
from blsh.database import engine, select_one, select_first, select_all, execute_batch

log = logging.getLogger(__name__)

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


def get_latest_biz_date(base_date: str = time.strftime("%Y%m%d")) -> str:
    """기준일 이전 최근 거래일 반환 (YYYYMMDD)"""
    row = select_one(
        "SELECT MAX(trd_dd) AS d FROM isu_ksp_ohlcv where trd_dd <= :bd", bd=base_date
    )
    return row["d"] if row else None


def find_next_biz_date_from_ohlcv(base_date: str) -> str:
    """isu_ksp_ohlcv에서 base_date의 다음 영업일 반환 (YYYYMMDD)"""
    row = select_one(
        "SELECT MIN(trd_dd) AS d FROM isu_ksp_ohlcv WHERE trd_dd > :bd", bd=base_date
    )
    return row["d"] if row else None


def find_next_opnday_from_holiday(base_date) -> str | None:
    """krx_holiday에서 base_date의 다음 영업일 반환 (YYYYMMDD)"""
    if select_first(
        """
            SELECT bass_dt FROM krx_holiday
            WHERE bass_dt = :bd 
        """,
        bd=base_date,
    ):
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
    return None


def save_holiday(df) -> int:
    """krx_holiday 테이블에 upsert. 건수 반환."""
    if df is None or df.empty:
        return 0

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

    return len(df)


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


# def save_signals(results):
#     if not results:
#         log.info("저장할 데이터 없음")
#         return

#     execute_batch(
#         """
#         INSERT INTO stock_signals (
#             base_date, target_date, ticker, name, market,
#             buy_score, mode, entry_price, stop_loss, take_profit,
#             close, atr, rsi, macd, macd_signal, macd_hist,
#             bb_upper, bb_middle, bb_lower, stoch_k, stoch_d,
#             foreign_netbuy, inst_netbuy, indi_netbuy,
#             buy_flags
#         ) VALUES (
#             %(base_date)s, %(target_date)s, %(ticker)s, %(name)s, %(market)s,
#             %(buy_score)s, %(mode)s, %(entry_price)s, %(stop_loss)s, %(take_profit)s,
#             %(close)s, %(atr)s, %(rsi)s, %(macd)s, %(macd_signal)s, %(macd_hist)s,
#             %(bb_upper)s, %(bb_middle)s, %(bb_lower)s, %(stoch_k)s, %(stoch_d)s,
#             %(foreign_netbuy)s, %(inst_netbuy)s, %(indi_netbuy)s,
#             %(buy_flags)s
#         )
#         ON CONFLICT (base_date, ticker) DO UPDATE SET
#             target_date    = EXCLUDED.target_date,
#             buy_score      = EXCLUDED.buy_score,
#             mode           = EXCLUDED.mode,
#             entry_price    = EXCLUDED.entry_price,
#             stop_loss      = EXCLUDED.stop_loss,
#             take_profit    = EXCLUDED.take_profit,
#             close          = EXCLUDED.close,
#             atr            = EXCLUDED.atr,
#             rsi            = EXCLUDED.rsi,
#             macd           = EXCLUDED.macd,
#             macd_signal    = EXCLUDED.macd_signal,
#             macd_hist      = EXCLUDED.macd_hist,
#             bb_upper       = EXCLUDED.bb_upper,
#             bb_middle      = EXCLUDED.bb_middle,
#             bb_lower       = EXCLUDED.bb_lower,
#             stoch_k        = EXCLUDED.stoch_k,
#             stoch_d        = EXCLUDED.stoch_d,
#             foreign_netbuy = EXCLUDED.foreign_netbuy,
#             inst_netbuy    = EXCLUDED.inst_netbuy,
#             indi_netbuy    = EXCLUDED.indi_netbuy,
#             buy_flags      = EXCLUDED.buy_flags
#         """,
#         results,
#     )
#     log.info(f"DB 저장 완료: {len(results)}건")


# def get_singnals(base_date):
#     return select_all(
#         """
#         SELECT *
#         FROM stock_signals
#         WHERE base_date = :bd
#         """,
#         bd=base_date,
#     )


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
        SELECT DISTINCT trd_dd
        FROM isu_ksp_ohlcv
        WHERE trd_dd >= :start
        ORDER BY trd_dd
        LIMIT :n
        """,
        **{"start": target_date, "n": max_hold_days},
    )


if __name__ == "__main__":
    print(get_max_hold_dates("20260314", 5))
