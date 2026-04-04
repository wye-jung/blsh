import time
from typing import Final
import numpy as np
import pandas as pd
from pykrx.website.krx.market.core import (
    투자자별_순매수상위종목,
    PER_PBR_배당수익률_전종목,
    전종목시세,
    외국인보유량_전종목,
)
from wye.blsh.krx.krx_data.market.core_ext import 전종목기본정보
from wye.blsh.krx.krx_data._base import Base


class Isu(Base):
    # mktid
    KOSPI: Final = "STK"
    KOSDAQ: Final = "KSQ"
    KONEX: Final = "KNX"
    ALL: Final = "ALL"

    def __init__(
        self,
        date=time.strftime("%Y%m%d"),
        mktid="STK",
        nearest=False,
    ):
        self.set_trd_dd(date, nearest)
        self.mktid = mktid

    def get_ohlcv(self, trd_dd=None, mktid=None):
        """
        티커, 시가, 고가, 저가, 종가, 거래량, 거래대금, 대비, 등락률, 시가총액, 상장주식수
        """
        trd_dd = trd_dd if trd_dd else self.trd_dd
        mktid = mktid if mktid else self.mktid
        cols = {
            "ISU_SRT_CD": str,
            "TDD_OPNPRC": np.int32,
            "TDD_HGPRC": np.int32,
            "TDD_LWPRC": np.int32,
            "TDD_CLSPRC": np.int32,
            "ACC_TRDVOL": np.int64,
            "ACC_TRDVAL": np.int64,
            "CMPPREVDD_PRC": np.int32,
            "FLUC_RT": np.float32,
            "MKTCAP": np.int64,
            "LIST_SHRS": np.int64,
        }
        df = 전종목시세().fetch(trd_dd, mktid)
        if (df[list(cols.keys())[1:-1]] == "-").all(axis=None):
            df = df.iloc[0:0]

        df = self.adjust_df(df, cols)
        df.insert(0, "trd_dd", trd_dd)
        return df

    def get_fundamental(self, trd_dd=None, mktid=None):
        """
        티커, BPS, PER, PBR, EPS, 배당수익률, DPS
        """
        trd_dd = trd_dd if trd_dd else self.trd_dd
        mktid = mktid if mktid else self.mktid
        cols = {
            "ISU_SRT_CD": str,
            "BPS": np.int32,
            "PER": np.float64,
            "PBR": np.float64,
            "EPS": np.int32,
            "DVD_YLD": np.float64,
            "DPS": np.int32,
        }
        df = PER_PBR_배당수익률_전종목().fetch(trd_dd, mktid)
        if (df[list(cols.keys())[1:]] == "-").all(axis=None):
            df = df.iloc[0:0]

        df = self.adjust_df(df, cols)
        df.insert(0, "trd_dd", trd_dd)
        return df

    def get_base_info(self, mktid=None):
        """
        '표준코드', '단축코드', '한글종목명', '한글종목약명', '영문종목명', '상장일',
        '시장구분', '증권구분', '소속부', '주식종류', '액면가',
        '상장주식수'
        """
        mktid = mktid if mktid else self.mktid
        cols = {
            "ISU_CD": str,
            "ISU_SRT_CD": str,
            "ISU_NM": str,
            "ISU_ABBRV": str,
            "ISU_ENG_NM": str,
            "LIST_DD": str,
            "MKT_TP_NM": str,
            "SECUGRP_NM": str,
            "SECT_TP_NM": str,
            "KIND_STKCERT_TP_NM": str,
            "PARVAL": np.int32,
            "LIST_SHRS": np.int64,
        }
        df = 전종목기본정보().fetch(mktid)
        df = self.adjust_df(df, cols)
        return df

    def get_market_net_purchases_of_equities(
        self,
        fromdate=None,
        todate=None,
        mktid=None,
        invstcd=9000,
    ):
        """
        invstcd:
        1000(금융투자), 2000(보험), 3000(투신), 3100(사모),
        4000(은행), 5000(기타금융), 6000(연기금), 7050(기관합계),
        7100(기타법인), 8000(개인), 9000(외국인) 9001(기타외국인),
        9999(전체),

        티커, 매도거래량, 매수거래량, 순매수거래량, 매도거래대금, 매수거래대금, 순매수거래대금
        """
        fromdate = fromdate if fromdate else self.trd_dd
        todate = todate if todate else self.trd_dd
        mktid = mktid if mktid else self.mktid
        cols = {
            "ISU_SRT_CD": str,
            "ASK_TRDVOL": np.int64,
            "BID_TRDVOL": np.int64,
            "NETBID_TRDVOL": np.int64,
            "ASK_TRDVAL": np.int64,
            "BID_TRDVAL": np.int64,
            "NETBID_TRDVAL": np.int64,
        }
        df = 투자자별_순매수상위종목().fetch(
            fromdate,
            todate,
            mktid,
            invstcd,
        )
        df = self.adjust_df(df, cols)
        df.insert(0, "invest_tp_cd", invstcd)
        if fromdate == todate:
            df.insert(0, "trd_dd", fromdate)
        return df

    def get_exhaustion_rates_of_foreign_investment(
        self, date=None, mktid=None, balance_limit=False
    ):
        """
        티커, 상장주식수, 보유수량, 지분율, 한도수량, 한도소진률
        """
        date = date if date else self.trd_dd
        mktid = mktid if mktid else self.mktid
        balance_limit = 1 if balance_limit else 0
        cols = {
            "ISU_SRT_CD": str,
            "LIST_SHRS": np.int64,
            "FORN_HD_QTY": np.int64,
            "FORN_SHR_RT": np.float16,
            "FORN_ORD_LMT_QTY": np.int64,
            "FORN_LMT_EXHST_RT": np.float16,
        }
        df = 외국인보유량_전종목().fetch(date, mktid, balance_limit)
        df = self.adjust_df(df, cols)
        df.insert(0, "trd_dd", date)
        return df

    def get_purchases_info(self, trd_dd=None, mktid=None):
        """
        외국인, 기관, 개인 순매수 및 외국인 한도소진율
        """
        trd_dd = trd_dd if trd_dd else self.trd_dd

        # 기관합계
        df1 = (
            self.get_market_net_purchases_of_equities(
                fromdate=trd_dd, todate=trd_dd, mktid=mktid, invstcd="7050"
            )[["isu_srt_cd", "netbid_trdvol"]]
            .set_index("isu_srt_cd")
            .rename(columns={"netbid_trdvol": "inst_netbid_trdvol"})
        )
        time.sleep(0.1)

        # 외국인
        df2 = (
            self.get_market_net_purchases_of_equities(
                fromdate=trd_dd, todate=trd_dd, mktid=mktid, invstcd="9000"
            )[["isu_srt_cd", "netbid_trdvol"]]
            .set_index("isu_srt_cd")
            .rename(columns={"netbid_trdvol": "frgn_netbid_trdvol"})
        )
        time.sleep(0.1)

        # 개인
        df3 = (
            self.get_market_net_purchases_of_equities(
                fromdate=trd_dd, todate=trd_dd, mktid=mktid, invstcd="8000"
            )[["isu_srt_cd", "netbid_trdvol"]]
            .set_index("isu_srt_cd")
            .rename(columns={"netbid_trdvol": "indi_netbid_trdvol"})
        )

        df = pd.concat([df1, df2, df3], axis=1).reset_index()
        df.insert(0, "trd_dd", trd_dd)
        df["fetched_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
        return df


if __name__ == "__main__":
    from wye.blsh.krx.krx_auth import login_krx

    login_krx()
    print(Isu("20260312").get_ohlcv().head())
