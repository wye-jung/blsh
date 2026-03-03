from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import Column, Float, Integer, BigInteger, String  # , DateTime, func


class Base(DeclarativeBase):
    # created = Column(DateTime, nullable=False, server_default=func.now())
    pass


class KrxDdTrd(Base):
    __tablename__ = "krx_dd_trd"
    __table_args__ = {"comment": "KRX 시리즈 일별시세정보"}
    bas_dd = Column(String(8), primary_key=True, comment="기준일자")
    idx_clss = Column(String(50), primary_key=True, comment="계열구분")
    idx_nm = Column(String(100), primary_key=True, comment="지수명")
    clsprc_idx = Column(Float, comment="종가")
    cmpprevdd_idx = Column(Float, comment="대비")
    fluc_rt = Column(Float, comment="등락률")
    opnprc_idx = Column(Float, comment="시가")
    hgprc_idx = Column(Float, comment="고가")
    lwprc_idx = Column(Float, comment="저가")
    acc_trdvol = Column(BigInteger, comment="거래량")
    acc_trdval = Column(BigInteger, comment="거래대금")
    mktcap = Column(BigInteger, comment="상장시가총액")


class KospiDdTrd(Base):
    __tablename__ = "kospi_dd_trd"
    __table_args__ = {"comment": "KOSPI 시리즈 일별시세정보"}
    bas_dd = Column(String(8), primary_key=True, comment="기준일자")
    idx_clss = Column(String(50), primary_key=True, comment="계열구분")
    idx_nm = Column(String(100), primary_key=True, comment="지수명")
    clsprc_idx = Column(Float, comment="종가")
    cmpprevdd_idx = Column(Float, comment="대비")
    fluc_rt = Column(Float, comment="등락률")
    opnprc_idx = Column(Float, comment="시가")
    hgprc_idx = Column(Float, comment="고가")
    lwprc_idx = Column(Float, comment="저가")
    acc_trdvol = Column(BigInteger, comment="거래량")
    acc_trdval = Column(BigInteger, comment="거래대금")
    mktcap = Column(BigInteger, comment="상장시가총액")


class KosdaqDdTrd(Base):
    __tablename__ = "kosdaq_dd_trd"
    __table_args__ = {"comment": "KOSDAQ 시리즈 일별시세정보"}
    bas_dd = Column(String(8), primary_key=True, comment="기준일자")
    idx_clss = Column(String(50), primary_key=True, comment="계열구분")
    idx_nm = Column(String(100), primary_key=True, comment="지수명")
    clsprc_idx = Column(Float, comment="종가")
    cmpprevdd_idx = Column(Float, comment="대비")
    fluc_rt = Column(Float, comment="등락률")
    opnprc_idx = Column(Float, comment="시가")
    hgprc_idx = Column(Float, comment="고가")
    lwprc_idx = Column(Float, comment="저가")
    acc_trdvol = Column(BigInteger, comment="거래량")
    acc_trdval = Column(BigInteger, comment="거래대금")
    mktcap = Column(BigInteger, comment="상장시가총액")


class StkByddTrd(Base):
    __tablename__ = "stk_bydd_trd"
    __table_args__ = {"comment": "유가증권 일별매매정보"}
    bas_dd = Column(String(8), primary_key=True, comment="기준일자")
    isu_cd = Column(String(8), primary_key=True, comment="종목코드")
    isu_nm = Column(String(50), comment="종목명")
    mkt_nm = Column(String(10), comment="시장구분")
    sect_tp_nm = Column(String(50), comment="소속부")
    tdd_clsprc = Column(Integer, comment="종가")
    cmpprevdd_prc = Column(Integer, comment="대비")
    fluc_rt = Column(Float, comment="등락률")
    tdd_opnprc = Column(Integer, comment="시가")
    tdd_hgprc = Column(Integer, comment="고가")
    tdd_lwprc = Column(Integer, comment="저가")
    acc_trdvol = Column(BigInteger, comment="거래량")
    acc_trdval = Column(BigInteger, comment="거래대금")
    mktcap = Column(BigInteger, comment="시가총액")
    list_shrs = Column(BigInteger, comment="상장주식수")


class KsqByddTrd(Base):
    __tablename__ = "ksq_bydd_trd"
    __table_args__ = {"comment": "코스닥 일별매매정보"}
    bas_dd = Column(String(8), primary_key=True, comment="기준일자")
    isu_cd = Column(String(8), primary_key=True, comment="종목코드")
    isu_nm = Column(String(50), comment="종목명")
    mkt_nm = Column(String(10), comment="시장구분")
    sect_tp_nm = Column(String(50), comment="소속부")
    tdd_clsprc = Column(Integer, comment="종가")
    cmpprevdd_prc = Column(Integer, comment="대비")
    fluc_rt = Column(Float, comment="등락률")
    tdd_opnprc = Column(Integer, comment="시가")
    tdd_hgprc = Column(Integer, comment="고가")
    tdd_lwprc = Column(Integer, comment="저가")
    acc_trdvol = Column(BigInteger, comment="거래량")
    acc_trdval = Column(BigInteger, comment="거래대금")
    mktcap = Column(BigInteger, comment="시가총액")
    list_shrs = Column(BigInteger, comment="상장주식수")


class EtfByddTrd(Base):
    __tablename__ = "etf_bydd_trd"
    __table_args__ = {"comment": "ETF 일별매매정보"}
    bas_dd = Column(String(8), primary_key=True, comment="기준일자")
    isu_cd = Column(String(8), primary_key=True, comment="종목코드")
    isu_nm = Column(String(100), comment="종목명")
    tdd_clsprc = Column(Integer, comment="종가")
    cmpprevdd_prc = Column(Integer, comment="대비")
    fluc_rt = Column(Float, comment="등락률")
    nav = Column(Float, comment="순자산가치(NAV)")
    tdd_opnprc = Column(Integer, comment="시가")
    tdd_hgprc = Column(Integer, comment="고가")
    tdd_lwprc = Column(Integer, comment="저가")
    acc_trdvol = Column(BigInteger, comment="거래량")
    acc_trdval = Column(BigInteger, comment="거래대금")
    mktcap = Column(BigInteger, comment="시가총액")
    invstasst_netasst_totamt = Column(BigInteger, comment="순자산총액")
    list_shrs = Column(BigInteger, comment="상장주식수")
    idx_ind_nm = Column(String(100), comment="기초지수_지수명")
    obj_stkprc_idx = Column(Float, comment="기초지수_종가")
    cmpprevdd_idx = Column(Float, comment="기초지수_대비")
    fluc_rt_idx = Column(Float, comment="기초지수_등락률")


class StkIsuBaseInfo(Base):
    __tablename__ = "stk_isu_base_info"
    __table_args__ = {"comment": "유가증권 종목기본정보"}
    isu_cd = Column(String(12), primary_key=True, comment="표준코드")
    isu_srt_cd = Column(String(8), comment="단축코드")
    isu_nm = Column(String(100), comment="한글 종목명")
    isu_abbrv = Column(String(50), comment="한글 종목약명")
    isu_eng_nm = Column(String(100), comment="영문 종목명")
    list_dd = Column(String(8), comment="상장일")
    mkt_tp_nm = Column(String(20), comment="시장구분")
    secugrp_nm = Column(String(20), comment="증권구분")
    sect_tp_nm = Column(String(50), comment="소속부")
    kind_stkcert_tp_nm = Column(String(20), comment="주식종류")
    parval = Column(String(10), comment="액면가")
    list_shrs = Column(BigInteger, comment="상장주식수")


class KsqIsuBaseInfo(Base):
    __tablename__ = "ksq_isu_base_info"
    __table_args__ = {"comment": "코스닥 종목기본정보"}
    isu_cd = Column(String(12), primary_key=True, comment="표준코드")
    isu_srt_cd = Column(String(8), comment="단축코드")
    isu_nm = Column(String(100), comment="한글 종목명")
    isu_abbrv = Column(String(50), comment="한글 종목약명")
    isu_eng_nm = Column(String(100), comment="영문 종목명")
    list_dd = Column(String(8), comment="상장일")
    mkt_tp_nm = Column(String(20), comment="시장구분")
    secugrp_nm = Column(String(20), comment="증권구분")
    sect_tp_nm = Column(String(50), comment="소속부")
    kind_stkcert_tp_nm = Column(String(20), comment="주식종류")
    parval = Column(String(10), comment="액면가")
    list_shrs = Column(BigInteger, comment="상장주식수")
