import time
from typing import Final
import numpy as np
from pykrx.website.krx.market.core import 전체지수시세
from wye.blsh.krx.krx_data._base import Base

# idx_clss:  지수 구분
# 01(KRX), 02(KOSPI), 03(KOSDAQ), 04(테마)


class Idx(Base):
    KRX: Final = "01"
    KOSPI: Final = "02"
    KOSDAQ: Final = "03"
    THEME: Final = "04"

    def __init__(
        self,
        date=time.strftime("%Y%m%d"),
        idx_clss=KRX,
        nearest=False,
    ):
        self.set_trd_dd(date, nearest)
        self.idx_clss = idx_clss

    def get_ohlcv(self, trd_dd=None, idx_clss=None):
        """
        지수명, 시가, 고가, 저가, 종가, 거래량, 거래대금, 대비, 등락율, 상장시가총액
        """
        trd_dd = trd_dd if trd_dd else self.trd_dd
        idx_clss = idx_clss if idx_clss else self.idx_clss
        cols = {
            "IDX_NM": str,
            "OPNPRC_IDX": np.float64,
            "HGPRC_IDX": np.float64,
            "LWPRC_IDX": np.float64,
            "CLSPRC_IDX": np.float64,
            "ACC_TRDVOL": np.int64,
            "ACC_TRDVAL": np.int64,
            "CMPPREVDD_IDX": np.float64,
            "FLUC_RT": np.float64,
            "MKTCAP": np.int64,
        }
        df = 전체지수시세().fetch(trd_dd, idx_clss)
        if (df[list(cols.keys())[1:2]] == "-").all(axis=None):
            df = df.iloc[0:0]

        df = self.adjust_df(df, cols)
        df.insert(0, "idx_clss", idx_clss)
        df.insert(0, "trd_dd", trd_dd)
        return df


if __name__ == "__main__":
    from wye.blsh.krx.krx_auth import login_krx

    login_krx()
    print(Idx(nearest=True).get_fundamental().head())
