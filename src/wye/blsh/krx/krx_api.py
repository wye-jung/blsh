import httpx
import asyncio
import time
import pandas as pd
from wye.blsh.common.env import KRX_API_KEY, KRX_API_URL


class Api:
    def __init__(self, cat, bas_dd=None):
        self.bas_dd = bas_dd if bas_dd else time.strftime("%Y%m%d")
        self.url = f"{KRX_API_URL}/{cat}"

    def get(self, endpoint):
        response = httpx.get(
            f"{self.url}/{endpoint}",
            headers={"AUTH_KEY": KRX_API_KEY},
            params={"bas_dd": self.bas_dd},
        )
        return self._adjust(pd.DataFrame(response.json()["OutBlock_1"]))

    async def aget(self, endpoint):
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.url}/{endpoint}",
                headers={"AUTH_KEY": KRX_API_KEY},
                params={"basDd": self.bas_dd},
            )
            return self._adjust(pd.DataFrame(response.json()["OutBlock_1"]))

    def _adjust(self, df):
        if not df.empty:
            df.columns = df.columns.str.lower()
            df = df.apply(
                lambda col: (
                    col.str.replace(",", "", regex=False).replace("", None)
                    if col.dtype == object
                    else col
                )
            )
        return df


class Idx(Api):
    def __init__(self, bas_dd=None):
        super().__init__("idx", bas_dd)

    # KRX 시리즈 일별시세정보
    async def get_krx_dd_trd(self):
        return await self.aget("krx_dd_trd")

    # KOSPI 시리즈 일별시세정보
    async def get_kospi_dd_trd(self):
        return await self.aget("kospi_dd_trd")

    # KOSDAQ 시리즈 일별시세정보
    async def get_kosdaq_dd_trd(self):
        return await self.aget("kosdaq_dd_trd")


class Sto(Api):
    def __init__(self, bas_dd=None):
        super().__init__("sto", bas_dd)

    # 유가증권 일별매매정보
    async def get_stk_bydd_trd(self):
        return await self.aget("stk_bydd_trd")

    # 코스닥 일별매매정보
    async def get_ksq_bydd_trd(self):
        return await self.aget("ksq_bydd_trd")

    # 유가증권 종목기본정보
    async def get_stk_isu_base_info(self):
        return await self.aget("stk_isu_base_info")

    # 코스닥 종목기본정보
    async def get_ksq_isu_base_info(self):
        return await self.aget("ksq_isu_base_info")


class Etp(Api):
    def __init__(self, bas_dd=None):
        super().__init__("etp", bas_dd)

    # ETF 일별매매정보
    async def get_etf_bydd_trd(self):
        return await self.aget("etf_bydd_trd")


if __name__ == "__main__":
    sto = Sto()
    df = asyncio.run(sto.get_ksq_bydd_trd())
    print(df.head())
