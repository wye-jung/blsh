import httpx
import asyncio
import pandas as pd
from blsh.common.env import KRX_API_KEY, KRX_API_URL


# def _get_krx_data(endpoint, params):
#     response = httpx.get(
#         f"{KRX_API_URL}/{endpoint}",
#         headers={"AUTH_KEY": KRX_API_KEY},
#         params=params,
#     )
#     df = pd.DataFrame(response.json()["OutBlock_1"])
#     df.columns = df.columns.str.lower()
#     return df


async def _get_krx_data_async(endpoint, params):
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{KRX_API_URL}/{endpoint}",
            headers={"AUTH_KEY": KRX_API_KEY},
            params=params,
        )
        df = pd.DataFrame(response.json()["OutBlock_1"])
        if not df.empty:
            df.columns = df.columns.str.lower()
            df = df.apply(
                lambda col: col.str.replace(",", "", regex=False).replace("", None)
                if col.dtype == object
                else col
            )
        return df


# KRX 시리즈 일별시세정보
async def get_krx_dd_trd(bas_dd):
    return await _get_krx_data_async("/idx/krx_dd_trd", {"basDd": bas_dd})


# KOSPI 시리즈 일별시세정보
async def get_kospi_dd_trd(bas_dd):
    return await _get_krx_data_async("/idx/kospi_dd_trd", {"basDd": bas_dd})


# KOSDAQ 시리즈 일별시세정보
async def get_kosdaq_dd_trd(bas_dd):
    return await _get_krx_data_async("/idx/kosdaq_dd_trd", {"basDd": bas_dd})


# 유가증권 일별매매정보
async def get_stk_bydd_trd(bas_dd):
    return await _get_krx_data_async("/sto/stk_bydd_trd", {"basDd": bas_dd})


# 코스닥 일별매매정보
async def get_ksq_bydd_trd(bas_dd):
    return await _get_krx_data_async("/sto/ksq_bydd_trd", {"basDd": bas_dd})


# 유가증권 종목기본정보
async def get_stk_isu_base_info(bas_dd):
    return await _get_krx_data_async("/sto/stk_isu_base_info", {"basDd": bas_dd})


# 코스닥 종목기본정보
async def get_ksq_isu_base_info(bas_dd):
    return await _get_krx_data_async("/sto/ksq_isu_base_info", {"basDd": bas_dd})


# ETF 일별매매정보
async def get_etf_bydd_trd(bas_dd):
    return await _get_krx_data_async("/etp/etf_bydd_trd", {"basDd": bas_dd})


if __name__ == "__main__":
    df = asyncio.run(get_krx_dd_trd("20260210"))
    print(df.head())
