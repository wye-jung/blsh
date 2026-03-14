# ─────────────────────────────────────────
# scanner 설정
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
ATR_SL_MULT = 2.0  # 손절: 종가 - 2.0×ATR
ATR_TP_MULT = 2.0  # 익절: 종가 + 2.0×ATR
GAP_THRESHOLD = 0.02
LOOKBACK_DAYS = 365  # 52주(252거래일) 신고가 계산을 위해 365일 이상 필요
MIN_SCORE = 1  # 저장 최소 점수
ENRICH_SCORE = 2  # 수급 보강 최소 점수

# 0단계 필터
TRDVAL_MIN = 1_000_000_000  # 최근 20일 평균 거래대금 최소값 (10억)
TRDVAL_DAYS = 20
INDEX_MA_DAYS = 20  # 지수 환경 체크 이동평균 기간

# ─────────────────────────────────────────
# reporter 설정
# ─────────────────────────────────────────
INVEST_MIN_SCORE = 9  # 투자 대상 선별 최소 점수
MAX_HOLD_DAYS = 10     # 미확정 시 최대 보유 거래일 (추세전환/REV)
MAX_HOLD_DAYS_MIX = 5  # MIX 모드 최대 보유 거래일
MAX_HOLD_DAYS_MOM = 3  # 모멘텀 모드 최대 보유 거래일
