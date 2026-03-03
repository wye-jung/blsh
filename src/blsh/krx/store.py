import blsh.krx.api as api
import blsh.database.engine as db
from blsh.database.models import (
    KrxDdTrd,
    KospiDdTrd,
    KosdaqDdTrd,
    StkByddTrd,
    KsqByddTrd,
    StkIsuBaseInfo,
    KsqIsuBaseInfo,
    EtfByddTrd,
)
import time


def _recreate(df, model, **filters):
    if df is not None and not df.empty:
        manager = db.ModelManager(model)
        manager.delete(**filters)
        manager.create(df)
        return len(df)
    else:
        print(f"No data to store for {model.__tablename__} with filters {filters}")
        return 0


# KRX 시리즈 일별시세정보
async def store_krx_dd_trd(bas_dd):
    return _recreate(await api.get_krx_dd_trd(bas_dd), KrxDdTrd, bas_dd=bas_dd)


# KOSPI 시리즈 일별시세정보
async def store_kospi_dd_trd(bas_dd):
    return _recreate(await api.get_kospi_dd_trd(bas_dd), KospiDdTrd, bas_dd=bas_dd)


# KOSDAQ 시리즈 일별시세정보
async def store_kosdaq_dd_trd(bas_dd):
    return _recreate(await api.get_kosdaq_dd_trd(bas_dd), KosdaqDdTrd, bas_dd=bas_dd)


# 유가증권 일별매매정보
async def store_stk_bydd_trd(bas_dd):
    return _recreate(await api.get_stk_bydd_trd(bas_dd), StkByddTrd, bas_dd=bas_dd)


# 코스닥 일별매매정보
async def store_ksq_bydd_trd(bas_dd):
    return _recreate(await api.get_ksq_bydd_trd(bas_dd), KsqByddTrd, bas_dd=bas_dd)


# 유가증권 종목기본정보
async def store_stk_isu_base_info(bas_dd):
    return _recreate(await api.get_stk_isu_base_info(bas_dd), StkIsuBaseInfo)


# 코스닥 종목기본정보
async def store_ksq_isu_base_info(bas_dd):
    return _recreate(await api.get_ksq_isu_base_info(bas_dd), KsqIsuBaseInfo)


# ETF 일별매매정보
async def store_etf_bydd_trd(bas_dd):
    return _recreate(await api.get_etf_bydd_trd(bas_dd), EtfByddTrd, bas_dd=bas_dd)


async def store_krx_today():
    bas_dd = time.strftime("%Y%m%d")
    count = await store_stk_isu_base_info(bas_dd)
    if count > 0:
        await store_ksq_isu_base_info(bas_dd)
        await store_kospi_dd_trd(bas_dd)
        await store_kosdaq_dd_trd(bas_dd)
        await store_stk_bydd_trd(bas_dd)
        await store_ksq_bydd_trd(bas_dd)
        await store_etf_bydd_trd(bas_dd)
    else:
        print(f"No KRX data to store for {bas_dd}")
