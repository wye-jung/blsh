"""
최종 최적 파라미터 비교 (2026-03-15기준 최근 1년 백테스트)

파라미터	DAY SWING
-------------------------------
INVEST_MIN_SCORE	12	11
ATR_SL_MULT	2.5	2.0
ATR_TP_MULT	2.0	2.0
MAX_HOLD_DAYS (REV)	1	7
MAX_HOLD_DAYS_MIX	1	4
MAX_HOLD_DAYS_MOM	1	2
총 수익률	+45.53%	+42.51%
승률	92.0%	72.41%
거래 수	1,153건	2,172건

특징 비교:
DAY: 소수 고확률 (93% 승률, 1일 청산)
SWING: 다수 분산 (72% 승률, REV 7일/MOM 2일), 거래량 2배
"""

# ─────────────────────────────────────────
# 트레이딩 모드  "DAY" | "SWING"
# ─────────────────────────────────────────
TRADE_FLAG = "DAY"

# ─────────────────────────────────────────
# scan 설정 (모드 무관 공통)
# ─────────────────────────────────────────
MACD_SHORT = 12
MACD_LONG = 26
MACD_SIGNAL = 9
RSI_PERIOD = 14
RSI_OVERSOLD = 30
BB_PERIOD = 20
BB_STD = 2.0
STOCH_K = 14
STOCH_D = 3
STOCH_SMOOTH = 3
MA_PERIODS = [5, 20, 60, 120]
ATR_PERIOD = 14
GAP_THRESHOLD = 0.02
LOOKBACK_DAYS = 365  # 52주(252거래일) 신고가 계산을 위해 365일 이상 필요
MIN_SCORE = 1  # 저장 최소 점수
ENRICH_SCORE = 2  # 수급 보강 최소 점수

# 0단계 필터
TRDVAL_MIN = 1_000_000_000  # 최근 20일 평균 거래대금 최소값 (10억)
TRDVAL_DAYS = 20
INDEX_MA_DAYS = 20  # 지수 환경 체크 이동평균 기간

# ─────────────────────────────────────────
# 모드별 factor
# ─────────────────────────────────────────
_DAY = {
    "INVEST_MIN_SCORE": 12,  # 데이트레이딩 최적화 결과 (1년 백테스트)
    "ATR_SL_MULT": 2.5,
    "ATR_TP_MULT": 2.0,
    "MAX_HOLD_DAYS": 1,
    "MAX_HOLD_DAYS_MIX": 1,
    "MAX_HOLD_DAYS_MOM": 1,
}

_SWING = {
    "INVEST_MIN_SCORE": 11,  # 스윙 트레이딩 최적화 결과 (1년 백테스트)
    "ATR_SL_MULT": 2.0,
    "ATR_TP_MULT": 2.0,
    "MAX_HOLD_DAYS": 7,
    "MAX_HOLD_DAYS_MIX": 4,
    "MAX_HOLD_DAYS_MOM": 2,
}

# ─────────────────────────────────────────
# 활성 factor 적용
# ─────────────────────────────────────────
_active = _DAY if TRADE_FLAG == "DAY" else _SWING

INVEST_MIN_SCORE = _active["INVEST_MIN_SCORE"]
ATR_SL_MULT = _active["ATR_SL_MULT"]
ATR_TP_MULT = _active["ATR_TP_MULT"]
MAX_HOLD_DAYS = _active["MAX_HOLD_DAYS"]
MAX_HOLD_DAYS_MIX = _active["MAX_HOLD_DAYS_MIX"]
MAX_HOLD_DAYS_MOM = _active["MAX_HOLD_DAYS_MOM"]
