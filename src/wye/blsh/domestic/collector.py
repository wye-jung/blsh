"""
ohlcv 데이터, 종목 기본정보 및 개장일 정보 수집
idx_stk_ohlcv, isu_ksp_ohlcv, isu_ksd_ohlcv, etf_ohlcv
idx_stk_info, isu_ksp_info, isu_ksd_info
stock_base_info, etf_base_info
krx_holidays

Note: 장중(08:00~20:00) 재실행 시 ohlcv는 실시간 갱신되지만,
      수급 데이터(purchases_info)는 KRX가 장중 업데이트하지 않을 수 있음.
      scanner의 KIS API fallback(fetch_investor_daily)이 당일 수급을 보완.
"""

import time
from pykrx.website import krx
from wye.blsh.database import ModelManager, query
from wye.blsh.common import dtutils
from wye.blsh.domestic import Milestone
from wye.blsh.common.env import SCAN_ETF
from wye.blsh.database.models import (
    IsuKspOhlcv,
    IsuKsdOhlcv,
    IdxStkOhlcv,
    EtfOhlcv,
    IsuKspInfo,
    IsuKsdInfo,
    IsuBaseInfo,
    EtfBaseInfo,
)
from wye.blsh.krx.krx_auth import login_krx
from wye.blsh.krx.krx_data import Idx, Isu, Etx
import logging

log = logging.getLogger(__name__)


def collect() -> tuple[bool, str]:
    login_krx()
    latest_biz_date = krx.get_nearest_business_day_in_a_week()
    max_ohlcv_date = query.get_max_ohlcv_date()
    from_date = None
    if max_ohlcv_date is None:
        from_date = latest_biz_date
    elif max_ohlcv_date < latest_biz_date:
        fetched_at = query.get_fetched_at(max_ohlcv_date)
        print(
            f"max_ohlcv_date: {max_ohlcv_date}, fetched_at: {fetched_at.strftime(dtutils.TIME_FMT)}"
        )
        if (
            fetched_at is not None
            and fetched_at.strftime(dtutils.TIME_FMT) <= Milestone.NXT_CLOSE_TIME
        ):
            from_date = max_ohlcv_date
        else:
            from_date = dtutils.add_biz_days(max_ohlcv_date, 1)
    elif max_ohlcv_date == latest_biz_date:
        if Milestone.NXT_OPEN_TIME < dtutils.ctime() < Milestone.NXT_CLOSE_TIME:
            _collect_idx_data(max_ohlcv_date)
            _collect_isu_data(max_ohlcv_date)
            if SCAN_ETF:
                _collect_etx_data(max_ohlcv_date)
        elif (_fetched_at := query.get_fetched_at(max_ohlcv_date)) is not None and (
            _fetched_at.strftime(dtutils.TIME_FMT) < Milestone.NXT_CLOSE_TIME
        ):
            from_date = max_ohlcv_date

    if from_date:
        _collect(from_date, latest_biz_date)

    max_ohlcv_date = query.get_max_ohlcv_date()
    return max_ohlcv_date == latest_biz_date, max_ohlcv_date


def _collect(from_date, to_date):
    log.info(f"_collect from {from_date} to {to_date}")
    import pandas as pd

    dates = pd.date_range(from_date, to_date)
    for date in dates[dates.weekday < 5].strftime(dtutils.DATE_FMT).tolist():
        if _collect_idx_data(date) > 0:
            _collect_isu_data(date)
            _collect_etx_data(date)

    _collect_base_info()


# 지수 데이터 수집
def _collect_idx_data(date):
    cnt = 0
    idx = Idx(date)
    for idx_clss in [Idx.KRX, Idx.KOSPI, Idx.KOSDAQ, Idx.THEME]:
        cnt += _recreate(
            idx.get_ohlcv(idx_clss=idx_clss),
            IdxStkOhlcv,
            trd_dd=idx.trd_dd,
            idx_clss=idx_clss,
        )
        if cnt == 0:
            break
        time.sleep(0.1)

    return cnt


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
def collect_holiday():
    dt = dtutils.add_days(days=30)
    if not query.get_krx_holiday(dt):
        from wye.blsh.kis import kis_auth as ka
        from wye.blsh.kis.domestic_stock import domestic_stock_functions as ds
        import pandas as pd

        ka.auth()
        base_date = query.get_krx_holiday_max_dt() or dtutils.add_days(days=-30)
        log.info(f"krx_holiday 미보유 ({base_date} 이후) → KIS API 조회")
        log.info(f"chk_holiday로 {base_date} 기준 약 100일치 데이터 반환")
        df = ds.chk_holiday(bass_dt=base_date)
        if df is not None and not df.empty:
            df["bass_dt"] = pd.to_datetime(df["bass_dt"]).dt.strftime(dtutils.DATE_FMT)
            query.save_holiday(df)


# 데이터 저장
def _recreate(df, model, **filters):
    if df is not None and not df.empty:
        manager = ModelManager(model)
        deleted = manager.delete(**filters)
        created = manager.create(df)
        log.info(
            f"Recreated {model.__tablename__} with filters {filters}: {deleted} deleted, {created} created"
        )
        time.sleep(0.1)
        return len(df)
    else:
        log.info(f"No data to store for {model.__tablename__} with filters {filters}")
        return 0


if __name__ == "__main__":
    collect()
