import time
import numpy as np
from pykrx.website.krx.etx.core import (
    ETF_전종목기본종목,
    전종목시세_ETF,
)
from blsh.krx.krx_data._base import Base


class Etx(Base):
    def __init__(
        self,
        date=time.strftime("%Y%m%d"),
        nearest=False,
    ):
        self.set_trd_dd(date, nearest)

    def get_etf_ohlcv(self, trd_dd=None):
        """
        종목코드, 종가, 대비, 등락률, 순자산가치(NAV), 시가, 고가, 저가, 거래량, 거래대금, 시가총액, 상장좌수,
        기초지수_명, 기초지수_종가, 기초지수_대비, 기초지수_등락율
        """
        trd_dd = trd_dd if trd_dd else self.trd_dd
        cols = {
            "ISU_SRT_CD": str,
            "TDD_CLSPRC": np.int32,
            "CMPPREVDD_PRC": np.int32,
            "FLUC_RT": np.int32,
            "NAV": np.float64,
            "TDD_OPNPRC": np.int32,
            "TDD_HGPRC": np.int32,
            "TDD_LWPRC": np.int32,
            "ACC_TRDVOL": np.int64,
            "ACC_TRDVAL": np.int64,
            "MKTCAP": np.int64,
            "LIST_SHRS": np.int32,
            "IDX_IND_NM": str,
            "OBJ_STKPRC_IDX": np.float64,
            "CMPPREVDD_IDX": np.float64,
            "FLUC_RT1": np.float64,
        }
        df = 전종목시세_ETF().fetch(trd_dd)
        if (df[list(cols.keys())[2:3]] == "-").all(axis=None):
            df = df.iloc[0:0]

        df = self.adjust_df(df, cols)
        df.insert(0, "trd_dd", trd_dd)
        df = df.rename(columns={"fluc_rt1": "fluc_rt_idx"})

        return df

    def get_etf_base_info(self):
        """
        표준코드, 단축코드, 한글종목명, 한글종목약명, 영문종목명, 상장일,
        기초지수명, 지수산출기관, 추적배수,
        복제방법, 기초시장분류, 기초자산분류,
        상장좌수, 운용사, CU수량, 총보수, 과세유형
        """
        cols = {
            "ISU_CD": str,
            "ISU_SRT_CD": str,
            "ISU_NM": str,
            "ISU_ABBRV": str,
            "ISU_ENG_NM": str,
            "LIST_DD": str,
            "ETF_OBJ_IDX_NM": str,
            "IDX_CALC_INST_NM1": str,
            "IDX_CALC_INST_NM2": str,
            "ETF_REPLICA_METHD_TP_CD": str,
            "IDX_MKT_CLSS_NM": str,
            "IDX_ASST_CLSS_NM": str,
            "LIST_SHRS": np.int64,
            "COM_ABBRV": str,
            "CU_QTY": np.int64,
            "ETF_TOT_FEE": np.float64,
            "TAX_TP_CD": str,
        }
        df = ETF_전종목기본종목().fetch()
        df = self.adjust_df(df, cols)
        return df


if __name__ == "__main__":
    from blsh.krx.krx_auth import login_krx

    login_krx()
    print(Etx(nearest=True).get_etf_ohlcv().head())
