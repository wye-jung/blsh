from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import (
    Column,
    Float,
    Integer,
    BigInteger,
    SmallInteger,
    String,
    Numeric,
    DateTime,
    func,
)
from wye.blsh.database import engine


class Base(DeclarativeBase):
    pass


class IdxStkOhlcv(Base):
    __tablename__ = "idx_stk_ohlcv"
    __table_args__ = {"comment": "주가지수 일별시세정보"}
    trd_dd = Column(String(8), primary_key=True, comment="기준일자")
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
    fetched_at = Column(DateTime, comment="API 조회 및 저장 일시")


class IsuKspOhlcv(Base):
    __tablename__ = "isu_ksp_ohlcv"
    __table_args__ = {"comment": "코스피 일별매매정보"}
    trd_dd = Column(String(8), primary_key=True, comment="기준일자")
    isu_srt_cd = Column(String(8), primary_key=True, comment="종목코드")
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
    fetched_at = Column(DateTime, comment="API 조회 및 저장 일시")


class IsuKsdOhlcv(Base):
    __tablename__ = "isu_ksd_ohlcv"
    __table_args__ = {"comment": "코스닥 일별매매정보"}
    trd_dd = Column(String(8), primary_key=True, comment="기준일자")
    isu_srt_cd = Column(String(8), primary_key=True, comment="종목코드")
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
    fetched_at = Column(DateTime, comment="API 조회 및 저장 일시")


class IsuKspInfo(Base):
    __tablename__ = "isu_ksp_info"
    __table_args__ = {"comment": "코스피 일별정보"}
    trd_dd = Column(String(8), primary_key=True, comment="기준일자")
    isu_srt_cd = Column(String(8), primary_key=True, comment="종목코드")
    inst_netbid_trdvol = Column(Float, comment="기관 순매수")
    frgn_netbid_trdvol = Column(Float, comment="외국인 순매수")
    indi_netbid_trdvol = Column(Float, comment="개인 순매수")
    fetched_at = Column(DateTime, comment="API 조회 및 저장 일시")


class IsuKsdInfo(Base):
    __tablename__ = "isu_ksd_info"
    __table_args__ = {"comment": "코스닥 일별정보"}
    trd_dd = Column(String(8), primary_key=True, comment="기준일자")
    isu_srt_cd = Column(String(8), primary_key=True, comment="종목코드")
    inst_netbid_trdvol = Column(Float, comment="기관 순매수")
    frgn_netbid_trdvol = Column(Float, comment="외국인 순매수")
    indi_netbid_trdvol = Column(Float, comment="개인 순매수")
    fetched_at = Column(DateTime, comment="API 조회 및 저장 일시")


class IsuBaseInfo(Base):
    __tablename__ = "isu_base_info"
    __table_args__ = {"comment": "종목기본정보"}
    isu_cd = Column(String(20), primary_key=True, comment="표준코드")
    isu_srt_cd = Column(String(8), index=True, comment="단축코드")
    isu_nm = Column(String, comment="한글종목명")
    isu_abbrv = Column(String, comment="한글종목약명")
    isu_eng_nm = Column(String, comment="영문종목명")
    list_dd = Column(String, comment="상장일")
    mkt_tp_nm = Column(String, comment="시장구분")
    secugrp_nm = Column(String, comment="증권구분")
    sect_tp_nm = Column(String, comment="소속부")
    kind_stkcert_tp_nm = Column(String, comment="주식종류")
    parval = Column(BigInteger, comment="액면가")
    list_shrs = Column(BigInteger, comment="상장주식수")
    fetched_at = Column(DateTime, comment="API 조회 및 저장 일시")


class EtfOhlcv(Base):
    __tablename__ = "etf_ohlcv"
    __table_args__ = {"comment": "ETF 일별매매정보"}
    trd_dd = Column(String(8), primary_key=True, comment="기준일자")
    isu_srt_cd = Column(String(8), primary_key=True, comment="종목코드")
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
    list_shrs = Column(BigInteger, comment="상장주식수")
    idx_ind_nm = Column(String(100), comment="기초지수_지수명")
    obj_stkprc_idx = Column(Float, comment="기초지수_종가")
    cmpprevdd_idx = Column(Float, comment="기초지수_대비")
    fluc_rt_idx = Column(Float, comment="기초지수_등락률")
    fetched_at = Column(DateTime, comment="API 조회 및 저장 일시")


class EtfBaseInfo(Base):
    __tablename__ = "etf_base_info"
    __table_args__ = {"comment": "ETF기본정보"}
    isu_cd = Column(String(20), primary_key=True, comment="표준코드")
    isu_srt_cd = Column(String(8), index=True, comment="단축코드")
    isu_nm = Column(String, comment="한글종목명")
    isu_abbrv = Column(String, comment="한글종목약명")
    isu_eng_nm = Column(String, comment="영문종목명")
    list_dd = Column(String, comment="상장일")
    etf_obj_idx_nm = Column(String, comment="기초지수명")
    idx_calc_inst_nm1 = Column(String, comment="지수산출기관")
    idx_calc_inst_nm2 = Column(String, comment="추적배수")
    etf_replica_methd_tp_cd = Column(String, comment="복제방법")
    idx_mkt_clss_nm = Column(String, comment="기초시장분류")
    idx_asst_clss_nm = Column(String, comment="기초자산분류")
    list_shrs = Column(BigInteger, comment="상장좌수")
    com_abbrv = Column(String, comment="운용사")
    cu_qty = Column(BigInteger, comment="CU수량")
    etf_tot_fee = Column(Float, comment="총보수")
    tax_tp_cd = Column(String, comment="과세유형")
    fetched_at = Column(DateTime, comment="API 조회 및 저장 일시")


class KrxHoliday(Base):
    __tablename__ = "krx_holiday"
    __table_args__ = {"comment": "KRX 휴장일 테이블"}
    bass_dt = Column(String(8), primary_key=True, comment="날짜 (YYYYMMDD)")
    wday_dvsn_cd = Column(
        String(2),
        index=True,
        comment="요일 코드 (01=일 02=월 03=화 04=수 05=목 06=금 07=토)",
    )
    bzdy_yn = Column(String(1), comment="영업일 여부 (Y/N)")
    opnd_yn = Column(String(1), comment="개장일 여부 (Y/N) - 매수 목표일 판단 기준")
    tr_day_yn = Column(String(1), comment="거래일 여부 (Y/N)")
    sttl_day_yn = Column(String(1), comment="결제일 여부 (Y/N)")
    fetched_at = Column(DateTime, comment="API 조회 및 저장 일시")


class TradeCandidates(Base):
    """
    거래 후보 종목
    """
    __tablename__ = "trade_candidates"
    __table_args__ = ({"comment": "거래 후보 종목 (PK: entry_date + po_type + ticker)"},)

    # 기본 키 (Composite Primary Key)
    entry_date = Column(
        String(8),
        primary_key=True,
        nullable=False,
        comment="매수 목표일 (base_date 다음 영업일)"
    )
    po_type = Column(
        String(7),
        primary_key=True,
        nullable=False,
        comment="po type (1: pre, 2: regular, 3: final)"
    )
    ticker = Column(
        String(20),
        primary_key=True,
        nullable=False,
        comment="종목코드 (단축코드 6자리)",
    )

    # 일반 정보
    name = Column(String(100), comment="한글종목약명 (isu_base_info.isu_abbrv)")
    market = Column(String(20), comment="시장구분 (KOSPI/KOSDAQ)")

    # 스캔 기준일
    base_date = Column(String(8), comment="매수 신호 스캔 기준일 (OHLCV 마지막 날짜)")
    # 점수 및 모드
    buy_score = Column(
        SmallInteger,
        default=0,
        comment="매수 신호 종합 점수 (1단계 기술지표 + 2단계 수급)",
    )
    mode = Column(
        String(10), comment="신호 성격: MOM(모멘텀) / REV(추세전환) / MIX(혼합) / WEAK"
    )
    # 가격 및 전략 지표
    entry_price = Column(
        Numeric, comment="매수 상단가 = 종가 + 0.5×ATR (이 가격 이하 매수)"
    )
    # 기술적 지표 상세
    atr = Column(Numeric, comment="ATR 14일 지수이동평균")
    atr_sl_mult = Column(Numeric, comment="ATR_SL_MULT")
    atr_tp_mult = Column(Numeric, comment="ATR_TP_MULT")
    max_hold_days = Column(SmallInteger, comment="최대 보유 영업일수")
    expiry_date = Column(String(8), comment="청산일(매수일 + 최대 보유 영업일수)")
    created_at = Column(DateTime, server_default=func.now(), comment="레코드 생성일시")


class TradeHistory(Base):
    """매매 이력"""

    __tablename__ = "trade_history"
    __table_args__ = {"comment": "매매 이력 (매수/매도 체결 기록)"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    side = Column(String(4), nullable=False, comment="buy / sell")
    ticker = Column(String(20), nullable=False, index=True, comment="종목코드")
    name = Column(String(100), comment="종목명")
    qty = Column(Integer, comment="체결 수량")
    price = Column(Numeric, comment="체결가 (매수: 지정가, 매도: 0=시장가)")
    reason = Column(String(200), comment="사유 (손절/1차익절/만기청산 등)")
    po_type = Column(String(10), comment="PO 유형 (pre/morning/final)")
    traded_at = Column(
        DateTime, server_default=func.now(), index=True, comment="체결 시각"
    )


def create_tables():
    Base.metadata.create_all(bind=engine)


if __name__ == "__main__":
    create_tables()
