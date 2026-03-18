import time
import asyncio
from blsh.database import ModelManager, query
from blsh.common import dtutils
from blsh.database.models import (
    create_tables,
    IsuKspOhlcv,
    IsuKsdOhlcv,
    IdxStkOhlcv,
    EtfOhlcv,
    IsuKspInfo,
    IsuKsdInfo,
    IsuBaseInfo,
    EtfBaseInfo,
)
from blsh.krx.krx_auth import login_krx
from blsh.krx.krx_data import Idx, Isu, Etx
import logging

log = logging.getLogger(__name__)


async def collect_ohlcv():
    login_krx()
    today = dtutils.today()
    _collect_idx_data(today)
    _collect_isu_data(today)


def collect(fromdate=None):
    create_tables()
    login_krx()

    today = dtutils.today()
    if fromdate is None:
        date_str = query.get_max_ohlcv_date()
        if date_str is None:
            fromdate = today
        elif date_str < today:
            fromdate = dtutils.nextday(date_str)
        else:
            fromdate = today

    if fromdate:
        _collect(fromdate, today)

    _collect_holiday()


def _collect(fromdate=dtutils.today(), todate=dtutils.today()):
    log.info(f"_collect from {fromdate} to {todate}")
    for d in query.get_biz_dates(fromdate=fromdate, todate=todate):
        date = d["d"]
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
        time.sleep(0.1)


# 종목 데이터 수집
def _collect_isu_data(date):
    isu = Isu(date)
    _recreate(isu.get_ohlcv(mktid=Isu.KOSPI), IsuKspOhlcv, trd_dd=isu.trd_dd)
    _recreate(isu.get_purchases_info(mktid=Isu.KOSPI), IsuKspInfo, trd_dd=isu.trd_dd)

    _recreate(isu.get_ohlcv(mktid=Isu.KOSDAQ), IsuKsdOhlcv, trd_dd=isu.trd_dd)
    _recreate(isu.get_purchases_info(mktid=Isu.KOSDAQ), IsuKsdInfo, trd_dd=isu.trd_dd)


# etf 데이터 수집
def _collect_etx_data(date):
    etx = Etx(date)
    _recreate(etx.get_etf_ohlcv(), EtfOhlcv, trd_dd=etx.trd_dd)


# 종목 및 etf 기본정보
def _collect_base_info():
    _recreate(Isu().get_base_info(mktid=Isu.ALL), IsuBaseInfo)
    _recreate(Etx().get_etf_base_info(), EtfBaseInfo)


# 휴장일 from KIS
def _collect_holiday():
    today = dtutils.today()
    if not query.get_krx_holiday(today):
        from blsh.kis import kis_auth as ka
        from blsh.kis.domestic_stock import domestic_stock_functions as ds
        import pandas as pd

        ka.auth()
        base_date = query.get_krx_holiday_max_dt()["d"]
        log.info(f"krx_holiday 미보유 ({base_date} 이후) → KIS API 조회")
        log.info(f"chk_holiday로 {base_date} 기준 약 100일치 데이터 반환")
        df = ds.chk_holiday(bass_dt=base_date)
        df["bass_dt"] = dtutils.strftime(pd.to_datetime(df["bass_dt"]).dt)
        query.save_holiday(df)


# 데이터 저장
def _recreate(df, model, **filters):
    if df is not None and not df.empty:
        manager = ModelManager(model)
        manager.delete(**filters)
        manager.create(df)
        time.sleep(0.1)
        return len(df)
    else:
        log.info(f"No data to store for {model.__tablename__} with filters {filters}")
        return 0


if __name__ == "__main__":
    asyncio.run(acollect())
