"""
매수 신호 스캐너 v11
─────────────────────────────────────────────────────
대상: KOSPI(isu_ksp_ohlcv) / KOSDAQ(isu_ksd_ohlcv)

사용법:
  python macd_scanner.py                  # 최근 영업일 기준
  python macd_scanner.py --date 20260312  # 날짜 지정

⚠️  장 마감 후(16:00 이후) 실행 권장
    장중 실행 시 DB 수급 데이터(잠정치)와 KIS API 데이터 신뢰도 저하

[0단계] 종목 필터 (scan_market SQL)
  - 최근 20일 평균 거래대금(acc_trdval) 10억 이상
  - 지수 환경 체크: KOSPI/KOSDAQ 20MA 아래이면 해당 시장 스킵

[1단계] DB 기반 OHLCV 지표 스캔                              flag   성격
  ┌─────────────────────────────────────────┬──────┬────────┬──────┐
  │ MACD 골든크로스                          │  +2  │ MGC    │  모멘텀│
  │ MACD 예상 골든크로스                     │  +1  │ MPGC   │  중립  │
  │ RSI 30 상향 돌파                         │  +2  │ RBO    │  전환  │
  │ RSI 과매도 (< 30)                        │  +1  │ ROV    │  전환  │
  │ 볼린저 하단 반등                         │  +1  │ BBL    │  전환  │
  │ 볼린저 중간선 상향 돌파                  │  +1  │ BBM    │  중립  │
  │ 거래량 급증 + 양봉 (2배)                 │  +1  │ VS     │  모멘텀│
  │ 이동평균 정배열 전환 (5>20>60)           │  +1  │ MAA    │  모멘텀│
  │ 스토캐스틱 과매도 교차                   │  +1  │ SGC    │  중립  │
  │ 52주 신고가 돌파 (20일 최대 거래량 돌파) │  +2  │ W52    │  모멘텀│
  │ 눌림목 패턴 (5MA 종가/저가 이탈 후 복귀) │  +2  │ PB     │  모멘텀│
  │ 망치형 캔들                              │  +1  │ HMR    │  전환  │
  │ 장대 양봉                                │  +2  │ LB     │  모멘텀│
  │ 모닝스타 (3일 반전 패턴)                 │  +2  │ MS     │  전환  │
  │ OBV 상승 추세 (3일 연속)                 │  +1  │ OBV    │  모멘텀│
  └─────────────────────────────────────────┴──────┴────────┴──────┘

  → mode 컬럼: MOM(모멘텀) / REV(추세전환) / MIX(혼합) / WEAK

[2단계] DB 수급 보강 (1단계 점수 2점 이상 종목만)
  isu_ksp_info / isu_ksd_info 최근 5일 수급 추이 판별
  DB 미보유 종목은 KIS API(investor_trade_by_stock_daily) fallback

  ┌──────────────────────────────────────────┬──────┬──────┐
  │ 외국인 순매수 전환 (N일 매도→오늘 매수)  │  +3  │ F_TRN│
  │ 기관   순매수 전환 (N일 매도→오늘 매수)  │  +3  │ I_TRN│
  │ 외국인 3일 이상 연속 순매수              │  +2  │ F_C3 │
  │ 기관   3일 이상 연속 순매수              │  +2  │ I_C3 │
  │ 외국인 오늘만 순매수                     │  +1  │ F_1  │
  │ 기관   오늘만 순매수                     │  +1  │ I_1  │
  │ 외국인+기관 동시 해당                    │  +1  │ FI   │
  │ 개인만 대량 순매수 (외인·기관 없을 때)   │  -1  │ P_OV │
  └──────────────────────────────────────────┴──────┴──────┘

출력: stock_signals 테이블 저장
─────────────────────────────────────────────────────
"""

import argparse
import logging
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

from blsh.common.env import DB_URL

# ─────────────────────────────────────────
# 설정
# ─────────────────────────────────────────
MACD_SHORT    = 12;  MACD_LONG = 26;  MACD_SIGNAL = 9
RSI_PERIOD    = 14;  RSI_OVERSOLD = 30
BB_PERIOD     = 20;  BB_STD = 2.0
STOCH_K       = 14;  STOCH_D = 3;    STOCH_SMOOTH = 3
MA_PERIODS    = [5, 20, 60, 120]
ATR_PERIOD    = 14
ATR_SL_MULT   = 1.5   # 손절: 종가 - 1.5×ATR
ATR_TP_MULT   = 3.0   # 익절: 종가 + 3.0×ATR
GAP_THRESHOLD = 0.02
LOOKBACK_DAYS = 365   # 52주(252거래일) 신고가 계산을 위해 365일 이상 필요
MIN_SCORE     = 1     # 저장 최소 점수
ENRICH_SCORE  = 2     # 수급 보강 최소 점수
INVEST_MIN_SCORE = 7  # 투자 대상 선별 최소 점수

# 0단계 필터
TRDVAL_MIN    = 1_000_000_000   # 최근 20일 평균 거래대금 최소값 (10억)
TRDVAL_DAYS   = 20
INDEX_MA_DAYS = 20              # 지수 환경 체크 이동평균 기간

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


# ─────────────────────────────────────────
# 날짜 유틸
# ─────────────────────────────────────────
def get_latest_biz_date(engine) -> str:
    """isu_ksp_ohlcv에서 가장 최근 거래일 반환 (YYYYMMDD)"""
    row = pd.read_sql(
        text("SELECT MAX(trd_dd) AS d FROM isu_ksp_ohlcv"),
        engine
    )
    return str(row["d"].iloc[0])


# ─────────────────────────────────────────
# KRX 휴장일 테이블 (krx_holiday)
# ─────────────────────────────────────────
CREATE_HOLIDAY_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS krx_holiday (
    bass_dt      VARCHAR(8)  PRIMARY KEY,
    wday_dvsn_cd VARCHAR(2),
    bzdy_yn      VARCHAR(1),
    opnd_yn      VARCHAR(1),
    tr_day_yn    VARCHAR(1),
    sttl_day_yn  VARCHAR(1),
    fetched_at   TIMESTAMP   DEFAULT NOW()
);

COMMENT ON TABLE  krx_holiday              IS 'KIS API 국내휴장일조회 캐시 (1일 1회 호출 제한)';
COMMENT ON COLUMN krx_holiday.bass_dt      IS '날짜 (YYYYMMDD)';
COMMENT ON COLUMN krx_holiday.wday_dvsn_cd IS '요일 코드 (01=일 02=월 03=화 04=수 05=목 06=금 07=토)';
COMMENT ON COLUMN krx_holiday.bzdy_yn      IS '영업일 여부 (Y/N)';
COMMENT ON COLUMN krx_holiday.opnd_yn      IS '개장일 여부 (Y/N) - 매수 목표일 판단 기준';
COMMENT ON COLUMN krx_holiday.tr_day_yn    IS '거래일 여부 (Y/N)';
COMMENT ON COLUMN krx_holiday.sttl_day_yn  IS '결제일 여부 (Y/N)';
COMMENT ON COLUMN krx_holiday.fetched_at   IS 'API 조회 및 저장 일시';
"""

def _fetch_holiday_from_kis(base_date: str) -> list[dict]:
    """
    blsh.kis의 chk_holiday로 base_date 기준 약 100일치 데이터 반환.

    호출 패턴:
        ka.auth()
        ds.chk_holiday(bass_dt="20260313")

    반환값: DataFrame 또는 list[dict]
      컬럼: bass_dt, wday_dvsn_cd, bzdy_yn, opnd_yn, tr_day_yn, sttl_day_yn
        wday_dvsn_cd: 01=일 02=월 03=화 04=수 05=목 06=금 07=토
        opnd_yn     : Y=개장일(매수 가능), N=휴장

    ⚠️  bass_dt 정규화 필수:
        ds.chk_holiday 반환 DataFrame의 bass_dt 컬럼이
        Timestamp 또는 "YYYY-MM-DD" 형식일 수 있음.
        krx_holiday(VARCHAR 8) 비교 시 YYYYMMDD 8자리 문자열로 저장해야
        문자열 대소 비교가 정상 동작함.
        ("2026-03-13" < "20260312" → 비교 오작동 방지)
    """
    from blsh.kis import kis_auth as ka
    from blsh.kis.domestic_stock import domestic_stock_functions as ds

    ka.auth()
    result = ds.chk_holiday(bass_dt=base_date)

    if result is None:
        raise RuntimeError("chk_holiday 반환값 없음")

    # DataFrame → bass_dt 정규화 → list[dict]
    if hasattr(result, "to_dict"):
        df = result.copy()
        # bass_dt가 Timestamp/datetime/"YYYY-MM-DD" 등 어떤 형식이든 YYYYMMDD로 통일
        df["bass_dt"] = pd.to_datetime(df["bass_dt"]).dt.strftime("%Y%m%d")
        return df.to_dict("records")

    # list[dict]인 경우에도 bass_dt 정규화
    if isinstance(result, list):
        normalized = []
        for r in result:
            r = dict(r)
            bd = str(r["bass_dt"])
            # "YYYY-MM-DD" → "YYYYMMDD"
            r["bass_dt"] = bd.replace("-", "")[:8]
            normalized.append(r)
        return normalized

    raise RuntimeError(f"chk_holiday 예상치 못한 반환 타입: {type(result)}")


def _save_holiday_to_db(engine, rows: list[dict]) -> int:
    """krx_holiday 테이블에 upsert. 저장된 건수 반환."""
    if not rows:
        return 0
    with engine.connect() as conn:
        cnt = 0
        for r in rows:
            conn.execute(text("""
                INSERT INTO krx_holiday
                    (bass_dt, wday_dvsn_cd, bzdy_yn, opnd_yn, tr_day_yn, sttl_day_yn)
                VALUES
                    (:bass_dt, :wday_dvsn_cd, :bzdy_yn, :opnd_yn, :tr_day_yn, :sttl_day_yn)
                ON CONFLICT (bass_dt) DO NOTHING
            """), {
                "bass_dt":      r["bass_dt"],
                "wday_dvsn_cd": r["wday_dvsn_cd"],
                "bzdy_yn":      r["bzdy_yn"],
                "opnd_yn":      r["opnd_yn"],
                "tr_day_yn":    r["tr_day_yn"],
                "sttl_day_yn":  r["sttl_day_yn"],
            })
            cnt += 1
        conn.commit()
    return cnt


def get_next_biz_date(engine, base_date: str) -> str:
    """
    base_date 다음 영업일 반환.

    처리 순서:
      1. isu_ksp_ohlcv에서 MIN(trd_dd) > base_date 조회
         → 과거 날짜 스캔 시 가장 정확 (실제 거래일 기반, 공휴일 자동 반영)
      2. 없으면 krx_holiday 테이블에서 opnd_yn='Y' 조회
         → 캐시 히트 시 KIS API 호출 불필요
      3. 없으면 KIS chk_holiday API 호출 → krx_holiday 저장 후 재조회
         → 오늘/미래 날짜 스캔 시 공휴일까지 정확히 반영
    """
    # ── 1순위: ohlcv 테이블
    row = pd.read_sql(
        text("SELECT MIN(trd_dd) AS d FROM isu_ksp_ohlcv WHERE trd_dd > :bd"),
        engine, params={"bd": base_date}
    )
    val = row["d"].iloc[0]
    if pd.notna(val):
        result = str(val)
        log.info(f"다음 영업일: {result}  [isu_ksp_ohlcv]")
        return result

    # ── 2순위: krx_holiday 테이블 (캐시)
    def _query_holiday_db() -> str | None:
        row = pd.read_sql(
            text("""
                SELECT bass_dt FROM krx_holiday
                WHERE bass_dt > :bd AND opnd_yn = 'Y'
                ORDER BY bass_dt
                LIMIT 1
            """),
            engine, params={"bd": base_date}
        )
        return str(row["bass_dt"].iloc[0]) if not row.empty else None

    result = _query_holiday_db()
    if result:
        log.info(f"다음 영업일: {result}  [krx_holiday 테이블]")
        return result

    # ── 3순위: KIS API 호출 → krx_holiday 저장 후 재조회
    log.info(f"krx_holiday 미보유 ({base_date} 이후) → KIS API 조회")
    try:
        rows  = _fetch_holiday_from_kis(base_date)
        saved = _save_holiday_to_db(engine, rows)
        log.info(f"krx_holiday 저장: {saved}건 ({rows[0]['bass_dt']} ~ {rows[-1]['bass_dt']})")
    except Exception as e:
        raise RuntimeError(f"KIS chk_holiday 호출 실패: {e}") from e

    result = _query_holiday_db()
    if result:
        log.info(f"다음 영업일: {result}  [KIS API → krx_holiday]")
        return result

    raise RuntimeError(f"다음 영업일 조회 실패: base_date={base_date}")


# ─────────────────────────────────────────
# DB 테이블
# ─────────────────────────────────────────
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stock_signals (
    base_date       VARCHAR(8)   NOT NULL,
    target_date     VARCHAR(8),
    ticker          VARCHAR(20)  NOT NULL,
    name            VARCHAR(100),
    market          VARCHAR(20),
    buy_score       SMALLINT     DEFAULT 0,
    mode            VARCHAR(10),
    entry_price     NUMERIC,
    stop_loss       NUMERIC,
    take_profit     NUMERIC,
    close           NUMERIC,
    atr             NUMERIC,
    rsi             NUMERIC,
    macd            NUMERIC,
    macd_signal     NUMERIC,
    macd_hist       NUMERIC,
    bb_upper        NUMERIC,
    bb_middle       NUMERIC,
    bb_lower        NUMERIC,
    stoch_k         NUMERIC,
    stoch_d         NUMERIC,
    foreign_netbuy  NUMERIC,
    inst_netbuy     NUMERIC,
    indi_netbuy     NUMERIC,
    buy_flags       TEXT,
    created         TIMESTAMP    DEFAULT NOW(),
    PRIMARY KEY (base_date, ticker)
);

COMMENT ON TABLE  stock_signals                IS '매수 신호 스캐너 결과 (PK: base_date + ticker)';
COMMENT ON COLUMN stock_signals.base_date      IS '스캔 기준일 (OHLCV 마지막 날짜)';
COMMENT ON COLUMN stock_signals.target_date    IS '매수 목표일 (base_date 다음 영업일)';
COMMENT ON COLUMN stock_signals.ticker         IS '종목코드 (단축코드 6자리)';
COMMENT ON COLUMN stock_signals.name           IS '한글종목약명 (isu_base_info.isu_abbrv)';
COMMENT ON COLUMN stock_signals.market         IS '시장구분 (KOSPI/KOSDAQ)';
COMMENT ON COLUMN stock_signals.buy_score      IS '매수 신호 종합 점수 (1단계 기술지표 + 2단계 수급)';
COMMENT ON COLUMN stock_signals.mode           IS '신호 성격: MOM(모멘텀) / REV(추세전환) / MIX(혼합) / WEAK';
COMMENT ON COLUMN stock_signals.entry_price    IS '매수 상단가 = 종가 + 0.5×ATR (이 가격 이하 매수)';
COMMENT ON COLUMN stock_signals.stop_loss      IS '손절가 = 종가 - 1.5×ATR';
COMMENT ON COLUMN stock_signals.take_profit    IS '익절가 = 종가 + 3.0×ATR';
COMMENT ON COLUMN stock_signals.close          IS '스캔일(base_date) 종가';
COMMENT ON COLUMN stock_signals.atr            IS 'ATR 14일 지수이동평균';
COMMENT ON COLUMN stock_signals.rsi            IS 'RSI 14';
COMMENT ON COLUMN stock_signals.macd           IS 'MACD (12-26)';
COMMENT ON COLUMN stock_signals.macd_signal    IS 'MACD 시그널선 (9일 EMA)';
COMMENT ON COLUMN stock_signals.macd_hist      IS 'MACD 히스토그램';
COMMENT ON COLUMN stock_signals.bb_upper       IS '볼린저밴드 상단 (20일, 2σ)';
COMMENT ON COLUMN stock_signals.bb_middle      IS '볼린저밴드 중간선 (20일 SMA)';
COMMENT ON COLUMN stock_signals.bb_lower       IS '볼린저밴드 하단 (20일, 2σ)';
COMMENT ON COLUMN stock_signals.stoch_k        IS '스토캐스틱 %K (14-3-3)';
COMMENT ON COLUMN stock_signals.stoch_d        IS '스토캐스틱 %D';
COMMENT ON COLUMN stock_signals.foreign_netbuy IS '외국인 순매수량 당일값 (isu_ksp/ksd_info)';
COMMENT ON COLUMN stock_signals.inst_netbuy    IS '기관 순매수량 당일값 (isu_ksp/ksd_info)';
COMMENT ON COLUMN stock_signals.indi_netbuy    IS '개인 순매수량 당일값 (isu_ksp/ksd_info)';
COMMENT ON COLUMN stock_signals.buy_flags      IS '발동된 신호 플래그 목록 (쉼표 구분)';
COMMENT ON COLUMN stock_signals.created        IS '레코드 생성일시';
"""

def init_db(engine):
    with engine.connect() as conn:
        conn.execute(text(CREATE_HOLIDAY_TABLE_SQL))
        conn.execute(text(CREATE_TABLE_SQL))
        conn.commit()
    log.info("DB 테이블 준비 완료 (krx_holiday, stock_signals)")


# ─────────────────────────────────────────
# 지표 계산
# ─────────────────────────────────────────
def calc_macd(c):
    es = c.ewm(span=MACD_SHORT, adjust=False).mean()
    el = c.ewm(span=MACD_LONG,  adjust=False).mean()
    m  = es - el
    s  = m.ewm(span=MACD_SIGNAL, adjust=False).mean()
    return m, s, m - s

def calc_rsi(c, p=RSI_PERIOD):
    d = c.diff()
    g = d.clip(lower=0).ewm(alpha=1/p, adjust=False).mean()
    l = (-d.clip(upper=0)).ewm(alpha=1/p, adjust=False).mean()
    return 100 - 100 / (1 + g / l.replace(0, np.nan))

def calc_bb(c, p=BB_PERIOD, k=BB_STD):
    m = c.rolling(p).mean()
    s = c.rolling(p).std()
    return m + k*s, m, m - k*s

def calc_atr(h, l, c, p=ATR_PERIOD):
    pc = c.shift(1)
    tr = pd.concat([h-l, (h-pc).abs(), (l-pc).abs()], axis=1).max(axis=1)
    return tr.ewm(span=p, adjust=False).mean()

def calc_stoch(h, l, c, k=STOCH_K, d=STOCH_D, sm=STOCH_SMOOTH):
    lo = l.rolling(k).min(); hi = h.rolling(k).max()
    rk = 100*(c-lo)/(hi-lo).replace(0, np.nan)
    pk = rk.rolling(sm).mean()
    return pk, pk.rolling(d).mean()

def calc_obv(c, v):
    sign = np.sign(c.diff()).fillna(0)
    return (sign * v).cumsum()


# ─────────────────────────────────────────
# 매수 신호 평가
# ─────────────────────────────────────────
def evaluate_buy(close, high, low, volume):
    min_len = MACD_LONG + MACD_SIGNAL + 5
    if len(close) < min_len:
        return 0, [], {}

    macd, sig, hist = calc_macd(close)
    rsi             = calc_rsi(close)
    bbu, bbm, bbl   = calc_bb(close)
    atr             = calc_atr(high, low, close)
    sk, sd          = calc_stoch(high, low, close)
    mas             = {p: close.rolling(p).mean() for p in MA_PERIODS}
    obv             = calc_obv(close, volume) if volume is not None else None

    c0, c1 = close.iloc[-1], close.iloc[-2]
    h0, h1 = high.iloc[-1],  high.iloc[-2]
    l0, l1 = low.iloc[-1],   low.iloc[-2]
    m0, m1 = macd.iloc[-1],  macd.iloc[-2]
    s0, s1 = sig.iloc[-1],   sig.iloc[-2]
    r0, r1 = rsi.iloc[-1],   rsi.iloc[-2]
    bbu0   = bbu.iloc[-1]
    bbm0, bbm1 = bbm.iloc[-1], bbm.iloc[-2]
    bbl0, bbl1 = bbl.iloc[-1], bbl.iloc[-2]
    sk0, sk1 = sk.iloc[-1], sk.iloc[-2]
    sd0, sd1 = sd.iloc[-1], sd.iloc[-2]
    atr0   = atr.iloc[-1]
    ma5    = mas[5];  ma20 = mas[20];  ma60 = mas[60]

    score = 0
    flags = []

    # 1. MACD 골든크로스 (+2)                                    → MGC
    if m0 > s0 and m1 < s1:
        score += 2; flags.append("MGC")
    # 2. MACD 예상 골든크로스 (+1)                               → MPGC
    elif (m0 < s0
          and len(hist) >= 3
          and hist.iloc[-3] < hist.iloc[-2] < hist.iloc[-1] < 0
          and abs(s0) > 0 and (s0-m0)/abs(s0) <= GAP_THRESHOLD):
        score += 1; flags.append("MPGC")

    # 3. RSI 30 상향 돌파 (+2)                                   → RBO
    if r0 > RSI_OVERSOLD and r1 <= RSI_OVERSOLD:
        score += 2; flags.append("RBO")
    # 4. RSI 과매도 (+1)                                         → ROV
    elif r0 < RSI_OVERSOLD:
        score += 1; flags.append("ROV")

    # 5. 볼린저 하단 반등 (+1)                                   → BBL
    if l1 < bbl1 and c0 > bbl0:
        score += 1; flags.append("BBL")

    # 6. 볼린저 중간선 상향 돌파 (+1)                            → BBM
    if c0 > bbm0 and c1 <= bbm1:
        score += 1; flags.append("BBM")

    # 7. 거래량 급증 + 양봉 (2배) (+1)                           → VS
    if volume is not None and len(volume) >= 20:
        vol_avg = volume.iloc[-20:-1].mean()
        if volume.iloc[-1] > vol_avg * 2 and c0 > c1:
            score += 1; flags.append("VS")

    # 8. 이동평균 정배열 전환 (5>20>60) (+1)                     → MAA
    if (ma5.iloc[-1] > ma20.iloc[-1] > ma60.iloc[-1]
            and not (ma5.iloc[-2] > ma20.iloc[-2] > ma60.iloc[-2])):
        score += 1; flags.append("MAA")

    # 9. 스토캐스틱 과매도 교차 (+1)                             → SGC
    if sk0 > sd0 and sk1 < sd1 and sk0 < 50:
        score += 1; flags.append("SGC")

    # 10. 52주 신고가 돌파 (+2) - 최근 20일 최대 거래량 돌파 시만  → W52
    if len(close) >= 252 and volume is not None and len(volume) >= 21:
        w52_high   = high.iloc[-252:-1].max()
        vol_20_max = volume.iloc[-21:-1].max()
        if h0 > w52_high and volume.iloc[-1] > vol_20_max:
            score += 2; flags.append("W52")

    # 11. 눌림목 패턴 (+2)                                       → PB
    # 20MA 상승 중 + 전일 종가 또는 저가가 5MA 아래(꼬리 눌림 포함)
    # + 오늘 종가 5MA 위 복귀 + 20MA 위 유지
    if (ma20.iloc[-1] > ma20.iloc[-5]
            and (c1 < ma5.iloc[-2] or l1 < ma5.iloc[-2])
            and c0 > ma5.iloc[-1]
            and c0 > ma20.iloc[-1]):
        score += 2; flags.append("PB")

    # 12. 망치형 캔들 (+1)                                       → HMR
    body         = abs(c0 - close.shift(1).iloc[-1])
    candle_range = h0 - l0
    if candle_range > 0:
        lower_wick = min(c0, c1) - l0
        upper_wick = h0 - max(c0, c1)
        if (lower_wick > candle_range * 0.5
                and upper_wick < candle_range * 0.1
                and body < candle_range * 0.3):
            score += 1; flags.append("HMR")

    # 13. 장대 양봉 (+2)                                         → LB
    body_size = c0 - close.shift(1).iloc[-1]
    if body_size > atr0 * 1.5:
        score += 2; flags.append("LB")

    # 14. 모닝스타 (3일 반전 패턴) (+2)                         → MS
    if len(close) >= 3:
        c_2, c_1, c_0 = close.iloc[-3], close.iloc[-2], close.iloc[-1]
        o_2     = close.shift(1).iloc[-3]
        body_d1 = o_2 - c_2
        body_d3 = c_0 - close.shift(1).iloc[-1]
        body_d2 = abs(c_1 - close.shift(1).iloc[-2])
        if (body_d1 > atr0 * 0.7
                and body_d2 < atr0 * 0.3
                and body_d3 > atr0 * 0.7
                and c_0 > (o_2 + c_2) / 2):
            score += 2; flags.append("MS")

    # 15. OBV 상승 추세 (3일 연속) (+1)                         → OBV
    if obv is not None and len(obv) >= 3:
        if obv.iloc[-3] < obv.iloc[-2] < obv.iloc[-1]:
            score += 1; flags.append("OBV")

    # ── 신호 성격 분류 (MOM/REV/MIX/WEAK)
    REVERSAL_FLAGS = {"ROV", "RBO", "BBL", "HMR", "MS"}
    MOMENTUM_FLAGS = {"MGC", "MAA", "W52", "PB", "LB", "VS", "OBV"}
    flag_set = set(flags)
    rev_cnt  = len(flag_set & REVERSAL_FLAGS)
    mom_cnt  = len(flag_set & MOMENTUM_FLAGS)
    if   mom_cnt >= 2 and mom_cnt > rev_cnt:  mode = "MOM"
    elif rev_cnt >= 2 and rev_cnt > mom_cnt:  mode = "REV"
    elif mom_cnt > 0  and rev_cnt > 0:        mode = "MIX"
    else:                                     mode = "WEAK"

    # ── 매수가 / 손절 / 익절
    entry_price = round(c0 + 0.5 * atr0, 2)
    stop_loss   = round(c0 - ATR_SL_MULT * atr0, 2)
    take_profit = round(c0 + ATR_TP_MULT * atr0, 2)

    indicators = {
        "mode":        mode,
        "close":       round(float(c0), 2),
        "atr":         round(float(atr0), 4),
        "rsi":         round(float(r0), 2),
        "macd":        round(float(m0), 4),
        "macd_signal": round(float(s0), 4),
        "macd_hist":   round(float(hist.iloc[-1]), 4),
        "bb_upper":    round(float(bbu0), 2),
        "bb_middle":   round(float(bbm0), 2),
        "bb_lower":    round(float(bbl0), 2),
        "stoch_k":     round(float(sk0), 2),
        "stoch_d":     round(float(sd0), 2),
        "entry_price": entry_price,
        "stop_loss":   stop_loss,
        "take_profit": take_profit,
    }
    return score, flags, indicators


# ─────────────────────────────────────────
# 공통: DataFrame → 신호 평가
# ─────────────────────────────────────────
def scan_dataframe(ticker, name, market, df, base_date,
                   close_col, high_col, low_col, vol_col=None):
    if df is None:
        return None

    df = df.sort_index().apply(pd.to_numeric, errors="coerce")

    # base_date 이하 데이터만 사용 (과거 날짜 지정 시 미래 데이터 차단)
    df = df[df.index <= base_date]

    if len(df) < LOOKBACK_DAYS // 3:
        return None

    close = df[close_col].dropna()
    high  = df[high_col].dropna()
    low   = df[low_col].dropna()
    vol   = df[vol_col].dropna() if vol_col and vol_col in df.columns else None

    idx   = close.index.intersection(high.index).intersection(low.index)
    close, high, low = close[idx], high[idx], low[idx]
    if vol is not None:
        vol = vol[idx]

    score, flags, ind = evaluate_buy(close, high, low, vol)
    if score < MIN_SCORE:
        return None

    icon = "🔴" if score >= 5 else "🟡" if score >= 3 else "🔵"
    log.info(f"  {icon} [{score:2d}pt] {ticker:10s} {name[:18]:18s} ({market}) {flags}")

    return {
        "base_date":      base_date,
        "target_date":    None,       # main()에서 채움
        "ticker":         ticker,
        "name":           name,
        "market":         market,
        "buy_score":      score,
        "buy_flags":      ",".join(flags),
        "foreign_netbuy": None,
        "inst_netbuy":    None,
        "indi_netbuy":    None,
        **ind,
    }


# ─────────────────────────────────────────
# 시장별 스캔
# ─────────────────────────────────────────
def load_name_map(engine):
    """isu_base_info에서 { isu_srt_cd: isu_abbrv } 맵 반환"""
    try:
        df = pd.read_sql(
            text("SELECT isu_srt_cd, isu_abbrv FROM isu_base_info"),
            engine
        )
        return dict(zip(df["isu_srt_cd"], df["isu_abbrv"]))
    except Exception as e:
        log.warning(f"종목명 로드 실패: {e}")
        return {}


def check_index_above_ma(engine, idx_nm, base_date, ma_days=INDEX_MA_DAYS):
    """
    idx_stk_ohlcv에서 base_date 기준 지수가 MA 위에 있는지 확인.
    True = 정상 (매수 환경), False = 하락장 (스캔 스킵)
    """
    try:
        df = pd.read_sql(
            text("""
                SELECT clsprc_idx
                FROM idx_stk_ohlcv
                WHERE idx_nm = :nm AND trd_dd <= :bd
                ORDER BY trd_dd DESC
                LIMIT :days
            """),
            engine, params={"nm": idx_nm, "bd": base_date, "days": ma_days + 1}
        )
        if len(df) < ma_days:
            return True
        prices = df["clsprc_idx"].astype(float).iloc[::-1]
        ma     = prices.mean()
        above  = float(prices.iloc[-1]) >= ma
        status = "위 ✅" if above else "아래 ⚠️"
        log.info(f"[지수 환경] {idx_nm}  현재가={prices.iloc[-1]:.2f}  "
                 f"{ma_days}MA={ma:.2f}  → {status}")
        return above
    except Exception as e:
        log.warning(f"지수 환경 체크 실패 ({idx_nm}): {e}")
        return True


def scan_market(engine, table, market, start, base_date, name_map,
                close_col="tdd_clsprc", high_col="tdd_hgprc",
                low_col="tdd_lwprc",   vol_col="acc_trdvol"):
    log.info(f"[{market}] {table} 스캔 시작  (기준일: {base_date})")

    # 0단계: 최근 TRDVAL_DAYS일 평균 거래대금 TRDVAL_MIN 이상 종목만 로드
    df_all = pd.read_sql(
        text(f"""
            SELECT o.isu_srt_cd, o.trd_dd,
                   o.{close_col}, o.{high_col}, o.{low_col},
                   o.{vol_col},  o.acc_trdval
            FROM {table} o
            WHERE o.trd_dd >= :start
              AND o.trd_dd <= :base_date
              AND o.isu_srt_cd IN (
                  SELECT isu_srt_cd
                  FROM {table}
                  WHERE trd_dd > :filter_start
                    AND trd_dd <= :base_date
                  GROUP BY isu_srt_cd
                  HAVING AVG(acc_trdval) >= :min_val
              )
            ORDER BY o.isu_srt_cd, o.trd_dd
        """),
        engine,
        params={
            "start":        start,
            "base_date":    base_date,
            "filter_start": (datetime.today() - timedelta(days=TRDVAL_DAYS * 2)).strftime("%Y%m%d"),
            "min_val":      TRDVAL_MIN,
        }
    )

    results = []
    for ticker, group in df_all.groupby("isu_srt_cd"):
        name = name_map.get(ticker, ticker)
        row  = scan_dataframe(ticker, name, market,
                              group.set_index("trd_dd"), base_date,
                              close_col, high_col, low_col, vol_col)
        if row:
            results.append(row)

    log.info(f"[{market}] 신호 종목: {len(results)}건 (거래대금 필터 후)")
    return results


# ─────────────────────────────────────────
# [2단계] DB 수급 보강 + KIS API fallback
# ─────────────────────────────────────────
def fetch_investor_daily(ticker, base_date, n_days=5):
    """
    blsh.kis - 종목별 투자자매매동향(일별)
    base_date 기준 최근 n_days 거래일의 외국인/기관 순매수량 반환.

    ds.investor_trade_by_stock_daily 파라미터:
      fid_cond_mrkt_div_code : J(KRX)
      fid_input_iscd         : 종목코드 6자리
      fid_input_date_1       : 기준일자 (YYYYMMDD)
      fid_org_adj_prc        : 공란
      fid_etc_cls_code       : 공란
      max_depth              : 1 (1페이지 = 최근 20~30일치, n_days 이상 충분)

    반환값 (output2 DataFrame):
      frgn_ntby_qty : 외국인 순매수량 (최신순)
      orgn_ntby_qty : 기관 순매수량 (최신순)

    반환: (frgn_list, orgn_list) - 오래된→최신 순, 각 n_days개
    """
    from blsh.kis import kis_auth as ka
    from blsh.kis.domestic_stock import domestic_stock_functions as ds

    try:
        ka.auth()
        result = ds.investor_trade_by_stock_daily(
            fid_cond_mrkt_div_code="J",
            fid_input_iscd=ticker,
            fid_input_date_1=base_date,
            fid_org_adj_prc="",
            fid_etc_cls_code="",
            tr_cont="",
            depth=0,
            max_depth=1,
        )

        # output2: DataFrame (최신순 정렬) 또는 None
        if result is None:
            return [], []

        # DataFrame인 경우
        if hasattr(result, "iloc"):
            df = result.head(n_days).iloc[::-1].reset_index(drop=True)  # 오래된→최신
            frgn = df["frgn_ntby_qty"].astype(float).astype(int).tolist()
            orgn = df["orgn_ntby_qty"].astype(float).astype(int).tolist()
            return frgn, orgn

        # output2가 (df1, df2) 튜플로 반환되는 경우
        if isinstance(result, tuple) and len(result) >= 2:
            df = result[1].head(n_days).iloc[::-1].reset_index(drop=True)
            frgn = df["frgn_ntby_qty"].astype(float).astype(int).tolist()
            orgn = df["orgn_ntby_qty"].astype(float).astype(int).tolist()
            return frgn, orgn

    except Exception as e:
        log.debug(f"  investor_daily 오류 ({ticker}): {e}")
    return [], []


def classify_supply(qty_list):
    """
    수급 흐름 분류 → (flag_suffix, score)
      TRN (+3): 직전 N-1일 순매도 → 오늘 순매수
      C3  (+2): 3일 이상 연속 순매수
      1   (+1): 오늘만 순매수
      None ( 0): 해당 없음
    """
    if not qty_list or len(qty_list) < 2:
        return None, 0
    today   = qty_list[-1]
    history = qty_list[:-1]
    if today <= 0:
        return None, 0
    prev = history[-1] if history else 0
    if prev <= 0:
        return "TRN", 3
    consec = 1
    for q in reversed(history):
        if q > 0: consec += 1
        else:     break
    if consec >= 3:
        return "C3", 2
    return "1", 1


def enrich_with_db(engine, results: list, base_date: str) -> list:
    """
    [2단계] isu_ksp_info / isu_ksd_info 에서 base_date 기준
    최근 5거래일 수급 판별 후 점수 보강.
    DB 미보유 종목은 KIS API fallback.
    """
    candidates = [r for r in results
                  if r["buy_score"] >= ENRICH_SCORE and r["market"] in ("KOSPI", "KOSDAQ")]
    if not candidates:
        return results

    log.info(f"[수급 보강] 대상 {len(candidates)}종목  (기준일: {base_date})")

    kospi_tickers  = [r["ticker"] for r in candidates if r["market"] == "KOSPI"]
    kosdaq_tickers = [r["ticker"] for r in candidates if r["market"] == "KOSDAQ"]

    def fetch_supply_from_db(table, tickers):
        if not tickers:
            return {}
        phs = ",".join([f"'{t}'" for t in tickers])
        sql = text(f"""
            SELECT isu_srt_cd, trd_dd,
                   frgn_netbid_trdvol AS frgn_qty,
                   inst_netbid_trdvol AS inst_qty,
                   indi_netbid_trdvol AS indi_qty
            FROM {table}
            WHERE isu_srt_cd IN ({phs})
              AND trd_dd <= :bd
            ORDER BY isu_srt_cd, trd_dd DESC
        """)
        try:
            df = pd.read_sql(sql, engine, params={"bd": base_date})
        except Exception as e:
            log.warning(f"  DB 수급 조회 오류 ({table}): {e}")
            return {}
        result = {}
        for ticker, grp in df.groupby("isu_srt_cd"):
            recent = grp.head(5).sort_values("trd_dd")
            result[ticker] = {
                "frgn":       recent["frgn_qty"].fillna(0).tolist(),
                "inst":       recent["inst_qty"].fillna(0).tolist(),
                "today_frgn": recent["frgn_qty"].iloc[-1] if len(recent) else 0,
                "today_inst": recent["inst_qty"].iloc[-1] if len(recent) else 0,
                "today_indi": recent["indi_qty"].iloc[-1] if len(recent) else 0,
            }
        return result

    supply_db = {
        **fetch_supply_from_db("isu_ksp_info", kospi_tickers),
        **fetch_supply_from_db("isu_ksd_info", kosdaq_tickers),
    }

    # KIS API fallback
    missing = [r for r in candidates if r["ticker"] not in supply_db]
    supply_api = {}
    if missing:
        log.info(f"  DB 미보유 {len(missing)}종목 → KIS API fallback")
        try:
            for row in missing:
                fl, ol = fetch_investor_daily(row["ticker"], base_date, n_days=5)
                if fl or ol:
                    supply_api[row["ticker"]] = {
                        "frgn":       fl,
                        "inst":       ol,
                        "today_frgn": fl[-1] if fl else 0,
                        "today_inst": ol[-1] if ol else 0,
                        "today_indi": None,
                    }
        except Exception as e:
            log.warning(f"  KIS API fallback 오류: {e}")

    supply_all    = {**supply_db, **supply_api}
    ticker_to_idx = {r["ticker"]: i for i, r in enumerate(results)}

    for row in candidates:
        t   = row["ticker"]
        idx = ticker_to_idx[t]
        sup = supply_all.get(t)
        if not sup:
            continue

        f_sig, f_sc = classify_supply(sup["frgn"])
        o_sig, o_sc = classify_supply(sup["inst"])

        results[idx]["foreign_netbuy"] = sup["today_frgn"]
        results[idx]["inst_netbuy"]    = sup["today_inst"]
        results[idx]["indi_netbuy"]    = sup.get("today_indi")

        if f_sc > 0:
            results[idx]["buy_score"] += f_sc
            results[idx]["buy_flags"] += f",F_{f_sig}"
            icon = "🔥" if f_sig == "TRN" else ("💰💰" if f_sig == "C3" else "💰")
            log.info(f"  {icon} 외국인 {f_sig}({f_sc:+d}): {t} {row['name']}  {sup['frgn']}")

        if o_sc > 0:
            results[idx]["buy_score"] += o_sc
            results[idx]["buy_flags"] += f",I_{o_sig}"
            icon = "🔥" if o_sig == "TRN" else ("🏦🏦" if o_sig == "C3" else "🏦")
            log.info(f"  {icon} 기관   {o_sig}({o_sc:+d}): {t} {row['name']}  {sup['inst']}")

        if f_sc > 0 and o_sc > 0:
            results[idx]["buy_score"] += 1
            results[idx]["buy_flags"] += ",FI"
            log.info(f"  ⭐ 외국인+기관 동시: {t} {row['name']}")

        indi = sup.get("today_indi") or 0
        frgn = sup["today_frgn"] or 0
        inst = sup["today_inst"] or 0
        if indi > 0 and frgn <= 0 and inst <= 0 and indi > abs(frgn) + abs(inst):
            results[idx]["buy_score"] -= 1
            results[idx]["buy_flags"] += ",P_OV"
            log.info(f"  ⚠️  개인 과매수 패널티(-1): {t} {row['name']}  개인={indi:+.0f}")

    return results


# ─────────────────────────────────────────
# PostgreSQL 저장
# ─────────────────────────────────────────
UPSERT_SQL = text("""
    INSERT INTO stock_signals (
        base_date, target_date, ticker, name, market,
        buy_score, mode, entry_price, stop_loss, take_profit,
        close, atr, rsi, macd, macd_signal, macd_hist,
        bb_upper, bb_middle, bb_lower, stoch_k, stoch_d,
        foreign_netbuy, inst_netbuy, indi_netbuy,
        buy_flags
    ) VALUES (
        :base_date, :target_date, :ticker, :name, :market,
        :buy_score, :mode, :entry_price, :stop_loss, :take_profit,
        :close, :atr, :rsi, :macd, :macd_signal, :macd_hist,
        :bb_upper, :bb_middle, :bb_lower, :stoch_k, :stoch_d,
        :foreign_netbuy, :inst_netbuy, :indi_netbuy,
        :buy_flags
    )
    ON CONFLICT (base_date, ticker) DO UPDATE SET
        target_date    = EXCLUDED.target_date,
        buy_score      = EXCLUDED.buy_score,
        mode           = EXCLUDED.mode,
        entry_price    = EXCLUDED.entry_price,
        stop_loss      = EXCLUDED.stop_loss,
        take_profit    = EXCLUDED.take_profit,
        close          = EXCLUDED.close,
        atr            = EXCLUDED.atr,
        rsi            = EXCLUDED.rsi,
        macd           = EXCLUDED.macd,
        macd_signal    = EXCLUDED.macd_signal,
        macd_hist      = EXCLUDED.macd_hist,
        bb_upper       = EXCLUDED.bb_upper,
        bb_middle      = EXCLUDED.bb_middle,
        bb_lower       = EXCLUDED.bb_lower,
        stoch_k        = EXCLUDED.stoch_k,
        stoch_d        = EXCLUDED.stoch_d,
        foreign_netbuy = EXCLUDED.foreign_netbuy,
        inst_netbuy    = EXCLUDED.inst_netbuy,
        indi_netbuy    = EXCLUDED.indi_netbuy,
        buy_flags      = EXCLUDED.buy_flags
""")

def save_to_db(engine, results):
    if not results:
        log.info("저장할 데이터 없음"); return
    with engine.connect() as conn:
        for row in results:
            conn.execute(UPSERT_SQL, row)
        conn.commit()
    log.info(f"DB 저장 완료: {len(results)}건")


# ─────────────────────────────────────────
# 출력 - 전체 요약
# ─────────────────────────────────────────
def print_general_summary(results):
    if not results: return
    df = pd.DataFrame(results)

    summary = df.groupby("market").agg(
        종목수=("ticker", "count"),
        평균점수=("buy_score", "mean"),
        최고점수=("buy_score", "max"),
        강한신호=("buy_score", lambda x: (x >= 5).sum()),
        외국인순매수=("foreign_netbuy", lambda x: (pd.to_numeric(x, errors="coerce") > 0).sum()),
        기관순매수=("inst_netbuy",     lambda x: (pd.to_numeric(x, errors="coerce") > 0).sum()),
    ).round(2).reset_index()
    log.info("\n─── 시장별 요약 ───\n" + summary.to_string(index=False))

    top = (df.sort_values("buy_score", ascending=False)
             .head(15)[["ticker", "name", "market", "buy_score", "mode",
                         "close", "entry_price", "stop_loss", "take_profit",
                         "foreign_netbuy", "inst_netbuy", "indi_netbuy", "buy_flags"]])
    log.info("\n─── 매수 신호 TOP15 ───\n" + top.to_string(index=False))


# ─────────────────────────────────────────
# 출력 - 투자 대상 선별 리포트
# ─────────────────────────────────────────
def print_invest_report(results, base_date):
    if not results:
        log.info("\n─── 투자 대상 없음 ───")
        return

    df = pd.DataFrame(results)
    for col in ("foreign_netbuy", "inst_netbuy", "indi_netbuy"):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    mask = (
        (df["buy_score"] >= INVEST_MIN_SCORE)
        & (df["mode"].isin(["MIX", "MOM"]))
        & ((df["foreign_netbuy"] > 0) | (df["inst_netbuy"] > 0))
        & (~df["buy_flags"].str.contains("P_OV", na=False))
    )
    candidates = df[mask].copy()

    candidates["_mode_rank"] = candidates["mode"].map({"MIX": 0, "MOM": 1}).fillna(2)

    def supply_strength(flags: str) -> int:
        if not isinstance(flags, str): return 0
        if "TRN" in flags: return 3
        if "C3"  in flags: return 2
        if "F_1" in flags or "I_1" in flags: return 1
        return 0

    candidates["_supply_rank"] = candidates["buy_flags"].apply(supply_strength)
    candidates = candidates.sort_values(
        ["_mode_rank", "_supply_rank", "buy_score"],
        ascending=[True, False, False]
    )

    sep = "═" * 110
    log.info(f"\n{sep}")
    log.info(f"  ★ 투자 대상 선별 리포트  |  기준일: {base_date}  |  총 {len(candidates)}종목")
    log.info(f"  선별 기준: score≥{INVEST_MIN_SCORE}  mode=MIX/MOM  수급(외인or기관)>0  P_OV 제외")
    log.info(sep)

    if candidates.empty:
        log.info("  해당 조건을 만족하는 종목이 없습니다.")
        log.info(sep)
        return

    for mode_label, mode_val in [("MIX (추세전환 초입 ★★★)", "MIX"),
                                  ("MOM (모멘텀 추종  ★★ )", "MOM")]:
        group = candidates[candidates["mode"] == mode_val]
        if group.empty:
            continue
        log.info(f"\n  【 {mode_label} 】  {len(group)}종목")
        log.info("  " + "─" * 108)

        for _, row in group.iterrows():
            frgn   = f"{row['foreign_netbuy']:+,.0f}" if pd.notna(row["foreign_netbuy"]) else "N/A"
            inst   = f"{row['inst_netbuy']:+,.0f}"    if pd.notna(row["inst_netbuy"])    else "N/A"
            indi   = f"{row['indi_netbuy']:+,.0f}"    if pd.notna(row["indi_netbuy"])    else "N/A"
            sl_gap = row["close"] - row["stop_loss"]
            rr     = (row["take_profit"] - row["close"]) / sl_gap if sl_gap > 0 else float("nan")
            rr_str = f"{rr:.1f}" if pd.notna(rr) else "N/A"
            log.info(
                f"  [{row['buy_score']:2d}pt] {row['ticker']}  {row['name'][:14]:<14s}  "
                f"{row['market']:<6s}  "
                f"종가 {row['close']:>8,.0f}  진입≤{row['entry_price']:>8,.0f}  "
                f"손절 {row['stop_loss']:>8,.0f}  익절 {row['take_profit']:>8,.0f}  "
                f"RR {rr_str}  "
                f"외인 {frgn:>12s}  기관 {inst:>12s}  개인 {indi:>12s}  "
                f"flags: {row['buy_flags']}"
            )

    log.info(f"\n{sep}\n")

    if not candidates.empty:
        log.info("  [ 선별 종목 분포 ]")
        log.info("  mode별:  " + "  ".join(f"{k}={v}" for k, v in candidates["mode"].value_counts().items()))
        log.info("  시장별:  " + "  ".join(f"{k}={v}" for k, v in candidates["market"].value_counts().items()))
        avg_rr = candidates.apply(
            lambda r: (r["take_profit"] - r["close"]) / (r["close"] - r["stop_loss"])
            if (r["close"] - r["stop_loss"]) > 0 else np.nan, axis=1
        ).mean()
        log.info(f"  평균 점수: {candidates['buy_score'].mean():.1f}점  평균 RR: {avg_rr:.2f}\n")


# ─────────────────────────────────────────
# 출력 - 수익률 리포트 (과거 날짜 스캔 시)
# ─────────────────────────────────────────
MAX_HOLD_DAYS = 5   # 미확정 시 최대 보유 거래일

def print_return_report(engine, results, base_date, target_date):
    """
    수익률 시뮬레이션 리포트.

    매수 시뮬레이션 규칙:
      - target_date open <= entry_price  → 시가 매수 (buy_price = open)
      - target_date open >  entry_price  → 갭 상승, 매수 불가

    결과 판정 - target_date 부터 최대 MAX_HOLD_DAYS 거래일 순차 확인:
      day_N low  <= stop_loss   → 손절 확정 (exit = stop_loss)
      day_N high >= take_profit → 익절 확정 (exit = take_profit)
      손익절 동시 발생 시       → 시가 기준 거리가 가까운 쪽 우선
      MAX_HOLD_DAYS 후에도 미확정 → 마지막 거래일 종가로 수익 판단
    """
    if not results:
        return
    if not target_date:
        log.info("[수익률 리포트] target_date 없음 (미래 날짜) → 스킵")
        return

    log.info(f"[수익률 리포트] 기준일={base_date}  목표일={target_date}  "
             f"최대 {MAX_HOLD_DAYS}거래일 추적")

    # 투자 대상 선별 기준 통과 종목만 대상
    df = pd.DataFrame(results)
    for col in ("foreign_netbuy", "inst_netbuy", "indi_netbuy"):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    cand_mask = (
        (df["buy_score"] >= INVEST_MIN_SCORE)
        & (df["mode"].isin(["MIX", "MOM"]))
        & ((df["foreign_netbuy"] > 0) | (df["inst_netbuy"] > 0))
        & (~df["buy_flags"].str.contains("P_OV", na=False))
    )
    candidates = df[cand_mask].copy()
    if candidates.empty:
        return

    tickers = candidates["ticker"].tolist()
    phs     = ",".join([f"'{t}'" for t in tickers])

    # ── target_date 이후 최대 MAX_HOLD_DAYS 거래일 날짜 목록 조회
    try:
        date_rows = pd.read_sql(
            text("""
                SELECT DISTINCT trd_dd
                FROM isu_ksp_ohlcv
                WHERE trd_dd >= :start
                ORDER BY trd_dd
                LIMIT :n
            """),
            engine,
            params={"start": target_date, "n": MAX_HOLD_DAYS}
        )
        hold_dates = date_rows["trd_dd"].tolist()
    except Exception as e:
        log.warning(f"[수익률 리포트] 거래일 조회 오류: {e}")
        return

    if not hold_dates:
        log.info(f"[수익률 리포트] {target_date} 이후 OHLCV 데이터 없음 → 스킵")
        return

    actual_days = len(hold_dates)
    log.info(f"  확인 기간: {hold_dates[0]} ~ {hold_dates[-1]}  ({actual_days}거래일)")

    # ── 해당 기간 전체 OHLCV 한 번에 조회
    date_list_str = ",".join([f"'{d}'" for d in hold_dates])

    def fetch_ohlcv_range(table):
        try:
            return pd.read_sql(
                text(f"""
                    SELECT isu_srt_cd AS ticker,
                           trd_dd,
                           tdd_opnprc AS open,
                           tdd_hgprc  AS high,
                           tdd_lwprc  AS low,
                           tdd_clsprc AS close
                    FROM {table}
                    WHERE trd_dd IN ({date_list_str})
                      AND isu_srt_cd IN ({phs})
                    ORDER BY isu_srt_cd, trd_dd
                """),
                engine
            )
        except Exception as e:
            log.warning(f"  OHLCV 조회 오류 ({table}): {e}")
            return pd.DataFrame()

    ohlcv_all = pd.concat(
        [fetch_ohlcv_range("isu_ksp_ohlcv"), fetch_ohlcv_range("isu_ksd_ohlcv")],
        ignore_index=True
    )

    # ticker → {trd_dd: {open, high, low, close}} 인덱스 구성
    ohlcv_idx: dict[str, dict[str, dict]] = {}
    for _, row in ohlcv_all.iterrows():
        ohlcv_idx.setdefault(row["ticker"], {})[row["trd_dd"]] = {
            "open":  float(row["open"]),
            "high":  float(row["high"]),
            "low":   float(row["low"]),
            "close": float(row["close"]),
        }

    rows_ok   = []   # 매수 진입 성공
    rows_gap  = []   # 갭 상승, 매수 불가
    rows_miss = []   # target_date 데이터 자체 없음

    for _, sig in candidates.iterrows():
        t       = sig["ticker"]
        entry   = float(sig["entry_price"])
        sl      = float(sig["stop_loss"])
        tp      = float(sig["take_profit"])
        days    = ohlcv_idx.get(t, {})

        # target_date 데이터 없음
        t1_ohv = days.get(hold_dates[0])
        if t1_ohv is None:
            rows_miss.append(sig.to_dict())
            continue

        # 갭 상승 체크: target_date 시가 > entry_price
        if t1_ohv["open"] > entry:
            rows_gap.append({**sig.to_dict(),
                             "t_open":     t1_ohv["open"],
                             "entry_date": hold_dates[0]})
            continue

        buy_price   = t1_ohv["open"]
        result_type = None
        exit_price  = None
        exit_date   = None
        last_ohv    = t1_ohv

        # 날짜 순서대로 손익절 확인
        for d in hold_dates:
            ohv = days.get(d)
            if ohv is None:
                continue
            last_ohv = ohv

            hit_sl = ohv["low"]  <= sl
            hit_tp = ohv["high"] >= tp

            if hit_sl and hit_tp:
                # 동일 캔들에서 손절/익절 동시 터치 → 시가와 가까운 쪽 우선
                if abs(buy_price - sl) <= abs(tp - buy_price):
                    result_type, exit_price = "손절", sl
                else:
                    result_type, exit_price = "익절", tp
            elif hit_sl:
                result_type, exit_price = "손절", sl
            elif hit_tp:
                result_type, exit_price = "익절", tp

            if result_type:
                exit_date = d
                break

        # MAX_HOLD_DAYS 후에도 미확정 → 마지막 거래일 종가
        if result_type is None:
            result_type = f"미확정({actual_days}일)"
            exit_price  = last_ohv["close"]
            exit_date   = hold_dates[-1]

        ret_pct = (exit_price - buy_price) / buy_price * 100
        rows_ok.append({
            **sig.to_dict(),
            "buy_price":   buy_price,
            "entry_date":  hold_dates[0],
            "exit_price":  exit_price,
            "exit_date":   exit_date,
            "result_type": result_type,
            "ret_pct":     ret_pct,
            "t_open":      t1_ohv["open"],
            "t_high":      last_ohv["high"],
            "t_low":       last_ohv["low"],
            "t_close":     last_ohv["close"],
        })

    # ── 출력
    sep = "═" * 115
    log.info(f"\n{sep}")
    log.info(f"  📊 수익률 리포트  |  기준일: {base_date}  →  목표일: {target_date}"
             f"  (최대 {MAX_HOLD_DAYS}거래일, 실제 {actual_days}거래일)")
    log.info(f"  대상: 선별 종목 {len(candidates)}개  "
             f"/ 매수 성공: {len(rows_ok)}  갭 상승(매수 불가): {len(rows_gap)}  "
             f"데이터 없음: {len(rows_miss)}")
    log.info(sep)

    if rows_ok:
        df_ok = pd.DataFrame(rows_ok).sort_values("ret_pct", ascending=False)
        wins  = df_ok[df_ok["result_type"] == "익절"]
        cuts  = df_ok[df_ok["result_type"] == "손절"]
        holds = df_ok[~df_ok["result_type"].isin(["익절", "손절"])]

        log.info(f"\n  ▶ 매수 성공 {len(df_ok)}종목  "
                 f"(익절 {len(wins)}  손절 {len(cuts)}  미확정 {len(holds)})")
        log.info("  " + "─" * 113)

        for _, r in df_ok.iterrows():
            if r["result_type"] == "익절":
                tag = "✅익절"
            elif r["result_type"] == "손절":
                tag = "❌손절"
            else:
                tag = f"⏳{r['result_type']}"

            log.info(
                f"  {tag:<10s}  [{r['buy_score']:2d}pt/{r['mode']}]  "
                f"{r['ticker']}  {r['name'][:12]:<12s}  {r['market']:<6s}  "
                f"매수 {r['buy_price']:>8,.0f} ({r['entry_date']})  "
                f"청산 {r['exit_price']:>8,.0f} ({r['exit_date']})  "
                f"수익률 {r['ret_pct']:>+6.2f}%"
            )

        avg_ret  = df_ok["ret_pct"].mean()
        win_rate = len(wins) / len(df_ok) * 100 if len(df_ok) else 0
        log.info(f"\n  평균 수익률: {avg_ret:+.2f}%  승률: {win_rate:.1f}%  "
                 f"(익절 {len(wins)} / 손절 {len(cuts)} / 미확정 {len(holds)})")

    if rows_gap:
        log.info(f"\n  ▶ 갭 상승 (매수 불가) {len(rows_gap)}종목")
        log.info("  " + "─" * 113)
        for r in rows_gap:
            log.info(f"  ⬆️갭상승  [{r['buy_score']:2d}pt/{r['mode']}]  "
                     f"{r['ticker']}  {r['name'][:12]:<12s}  "
                     f"진입가 {r['entry_price']:>8,.0f}  "
                     f"시가 {r['t_open']:>8,.0f} ({r['entry_date']})")

    log.info(f"\n{sep}\n")


# ─────────────────────────────────────────
# 메인
# ─────────────────────────────────────────
def parse_args():
    parser = argparse.ArgumentParser(description="매수 신호 스캐너")
    parser.add_argument(
        "--date", type=str, default=None,
        metavar="YYYYMMDD",
        help="스캔 기준일 (예: 20260312). 미지정 시 DB 최근 영업일 사용."
    )
    return parser.parse_args()


def main():
    args   = parse_args()
    engine = create_engine(DB_URL)
    init_db(engine)

    # ── 기준일 결정
    if args.date:
        base_date = args.date
        log.info(f"기준일 (지정): {base_date}")
    else:
        base_date = get_latest_biz_date(engine)
        log.info(f"기준일 (최근 영업일): {base_date}")

    # ── 다음 영업일 (매수 목표일)
    target_date = get_next_biz_date(engine, base_date)
    today_str   = datetime.today().strftime("%Y%m%d")
    is_today    = (base_date == today_str)

    if target_date:
        src = "캘린더(공휴일 미반영)" if is_today else "DB"
        log.info(f"매수 목표일: {target_date}  [{src}]")

    # ── 장중 실행 경고 (오늘 날짜 기준 스캔일 때만)
    if is_today:
        now = datetime.now()
        if now.weekday() < 5 and now.hour < 16:
            log.warning("⚠️  장중 실행 감지 (%s). 수급 데이터가 잠정치일 수 있습니다.",
                        now.strftime("%H:%M"))

    start    = (datetime.strptime(base_date, "%Y%m%d") - timedelta(days=LOOKBACK_DAYS)).strftime("%Y%m%d")
    name_map = load_name_map(engine)

    # ── 1단계: OHLCV 기술지표 스캔 (0단계 필터 포함)
    results = []

    if check_index_above_ma(engine, "코스피", base_date):
        results += scan_market(engine, "isu_ksp_ohlcv", "KOSPI",  start, base_date, name_map)
    else:
        log.warning("[KOSPI] 지수 20MA 아래 → 스캔 스킵")

    if check_index_above_ma(engine, "코스닥", base_date):
        results += scan_market(engine, "isu_ksd_ohlcv", "KOSDAQ", start, base_date, name_map)
    else:
        log.warning("[KOSDAQ] 지수 20MA 아래 → 스캔 스킵")

    # ── target_date 채우기
    for r in results:
        r["target_date"] = target_date

    # ── 2단계: DB 수급 보강
    results = enrich_with_db(engine, results, base_date)

    save_to_db(engine, results)
    print_general_summary(results)
    print_invest_report(results, base_date)
    print_return_report(engine, results, base_date, target_date)
    log.info(f"전체 완료: 총 {len(results)}건  기준일={base_date}  목표일={target_date}")


if __name__ == "__main__":
    main()
