import time
from datetime import datetime, timedelta
from pykrx import stock
from blsh.database import ModelManager, select_one, query
from blsh.database.models import (
    create_tables,
    IsuKspOhlcv,
    IsuKsdOhlcv,
    IdxStkOhlcv,
    EtfOhlcv,
    IsuKspInfo,
    IsuKsdInfo,
    IdxStkInfo,
    IsuBaseInfo,
    EtfBaseInfo,
)
from blsh.krx.krx_auth import login_krx
from blsh.krx.krx_data import Idx, Isu, Etx
import logging

log = logging.getLogger(__name__)

_date_fmt = "%Y%m%d"


def collect(fromdate=None):
    create_tables()
    login_krx()
    today = datetime.now().date()
    if fromdate is None:
        date_str = select_one("select max(trd_dd) As d from idx_stk_ohlcv")["d"]
        if date_str is None:
            fromdate = today.strftime(_date_fmt)
        else:
            date_obj = datetime.strptime(date_str, _date_fmt).date()
            if date_obj < today:
                fromdate = (date_obj + timedelta(days=1)).strftime(_date_fmt)

    if fromdate:
        _collect(fromdate, today.strftime(_date_fmt))


def _collect(fromdate=time.strftime(_date_fmt), todate=time.strftime(_date_fmt)):
    log.info(f"_collect from {fromdate} to {todate}")
    for d in stock.get_previous_business_days(fromdate=fromdate, todate=todate):
        date = d.strftime(_date_fmt)
        print(date)
        _collect_idx_data(date)
        _collect_isu_data(date)
        _collect_etx_data(date)

    _collect_base_info()


# 지수 데이터 수집
def _collect_idx_data(date):
    idx = Idx(date)
    for idx_clss in [Idx.KRX, Idx.KOSPI, Idx.KOSDAQ, Idx.THEME]:
        _recreate(
            idx.get_ohlcv(idx_clss=idx_clss),
            IdxStkOhlcv,
            trd_dd=idx.trd_dd,
            idx_clss=idx_clss,
        )
        _recreate(
            idx.get_fundamental(idx_clss=idx_clss),
            IdxStkInfo,
            trd_dd=idx.trd_dd,
            idx_clss=idx_clss,
        )


# 종목 데이터 수집
def _collect_isu_data(date):
    isu = Isu(date)
    _recreate(isu.get_ohlcv(mktid=Isu.KOSPI), IsuKspOhlcv, trd_dd=isu.trd_dd)
    _recreate(isu.get_daily_info(mktid=Isu.KOSPI), IsuKspInfo, trd_dd=isu.trd_dd)

    _recreate(isu.get_ohlcv(mktid=Isu.KOSDAQ), IsuKsdOhlcv, trd_dd=isu.trd_dd)
    _recreate(isu.get_daily_info(mktid=Isu.KOSDAQ), IsuKsdInfo, trd_dd=isu.trd_dd)


# etf 데이터 수집
def _collect_etx_data(date):
    etx = Etx(date)
    _recreate(etx.get_etf_ohlcv(), EtfOhlcv, trd_dd=etx.trd_dd)


# 종목 및 etf 기본정보
def _collect_base_info():
    _recreate(Isu().get_base_info(mktid=Isu.ALL), IsuBaseInfo)
    _recreate(Etx().get_etf_base_info(), EtfBaseInfo)


# 휴장일 from KIS
def collect_holiday_if_not_exists(base_date: str) -> str:
    next_opnday = query.find_next_opnday_from_holiday(base_date)
    if not next_opnday:
        from blsh.kis import kis_auth as ka
        from blsh.kis.domestic_stock import domestic_stock_functions as ds
        import pandas as pd

        ka.auth()
        log.info(f"krx_holiday 미보유 ({base_date} 이후) → KIS API 조회")
        print(f"chk_holiday로 {base_date} 기준 약 100일치 데이터 반환")
        df = ds.chk_holiday(bass_dt=base_date)
        df["bass_dt"] = pd.to_datetime(df["bass_dt"]).dt.strftime(_date_fmt)
        query.save_holiday(df)
    return query.find_next_opnday_from_holiday(base_date)


# 데이터 저장
def _recreate(df, model, **filters):
    if df is not None and not df.empty:
        manager = ModelManager(model)
        manager.delete(**filters)
        manager.create(df)
        time.sleep(0.1)
        return len(df)
    else:
        print(f"No data to store for {model.__tablename__} with filters {filters}")
        return 0


# def _is_holiday(date=time.strftime("%Y%m%d")):
#     return krx.get_index_ohlcv_by_date(date, date, "1001").empty

if __name__ == "__main__":
    login_krx()
    collect("20260313")
