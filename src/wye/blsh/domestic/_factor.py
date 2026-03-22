"""
최적 파라미터 (2026-03-22 기준, 최근 2년 백테스트)

파라미터              DAY     SWING
──────────────────────────────────────
INVEST_MIN_SCORE     12      11
ATR_SL_MULT          2.5     2.0
ATR_TP_MULT          2.0     2.0
TP1_MULT             0.7     1.0
TP1_RATIO            0.7     0.5
GAP_DOWN_LIMIT       0.03    0.03
MAX_HOLD_DAYS(REV)   1       7
MAX_HOLD_DAYS_MIX    1       3
MAX_HOLD_DAYS_MOM    0       1
SECTOR_PENALTY       -5%/-2  -5%/-2
SECTOR_BONUS         +1      +1

실행 후 grid_search 최적값으로 갱신할 것:
  uv run python -m wye.blsh.domestic.optimize.grid_search
"""

import os

# ─────────────────────────────────────────
# 트레이딩 모드  "DAY" | "SWING"
# ─────────────────────────────────────────
TRADE_FLAG = os.environ.get("TRADE_FLAG", "SWING").upper()

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
W52_VOL_MULT = 1.5  # 52주 신고가 거래량 조건: 20일 평균의 N배
LOOKBACK_DAYS = 365  # 52주(252거래일) 신고가 계산을 위해 365일 이상 필요
MIN_SCORE = 1  # 저장 최소 점수
ENRICH_SCORE = 2  # 수급 보강 최소 점수

# 0단계 필터
TRDVAL_MIN = 1_000_000_000  # 최근 20일 평균 거래대금 최소값 (10억)
TRDVAL_DAYS = 20
INDEX_MA_DAYS = 20       # 지수 환경 체크 이동평균 기간
INDEX_DROP_LIMIT = 0.05  # MA 대비 괴리율 -5% 이하일 때만 시장 전체 스캔 스킵 (재앙 수준)

# ─────────────────────────────────────────
# 업종코드 → DB idx_stk_ohlcv 지수명 매핑
# ─────────────────────────────────────────
KOSPI_MID_TO_IDX = {
    5: "음식료\u00b7담배", 6: "섬유\u00b7의류", 7: "종이\u00b7목재", 8: "화학",
    9: "제약", 10: "비금속", 11: "금속", 12: "기계\u00b7장비",
    13: "전기전자", 14: "의료\u00b7정밀기기", 15: "운송장비\u00b7부품",
    24: "증권", 25: "보험",
}
KOSPI_BIG_TO_IDX = {
    16: "유통", 17: "전기\u00b7가스", 18: "건설", 19: "운송\u00b7창고",
    20: "통신", 21: "금융", 26: "일반서비스", 27: "제조",
    28: "부동산", 29: "IT 서비스", 30: "오락\u00b7문화",
}

# ─────────────────────────────────────────
# 모드별 factor (grid_search 최적화 결과 반영)
# ─────────────────────────────────────────
_DAY = {
    "INVEST_MIN_SCORE": 12,
    "ATR_SL_MULT": 2.5,
    "ATR_TP_MULT": 2.0,
    "TP1_MULT": 0.7,            # 1차 익절: buy + ATR × TP1_MULT
    "TP1_RATIO": 0.7,           # 1차 익절 매도 비율 (1.0 = 전량)
    "GAP_DOWN_LIMIT": 0.03,     # 갭하락 3% 이상 시 매수 스킵
    "MAX_HOLD_DAYS": 1,         # 초단기 스윙 (DAY에서 오버나이트 허용)
    "MAX_HOLD_DAYS_MIX": 1,
    "MAX_HOLD_DAYS_MOM": 0,
    "SECTOR_PENALTY_THRESHOLD": -0.05,  # 업종지수 MA20 대비 -5% 이하
    "SECTOR_PENALTY_PTS": -2,
    "SECTOR_BONUS_PTS": 1,              # 업종지수 MA20 이상일 때 +1
}

_SWING = {
    "INVEST_MIN_SCORE": 11,
    "ATR_SL_MULT": 2.0,
    "ATR_TP_MULT": 2.0,
    "TP1_MULT": 1.0,
    "TP1_RATIO": 0.5,
    "GAP_DOWN_LIMIT": 0.03,
    "MAX_HOLD_DAYS": 7,
    "MAX_HOLD_DAYS_MIX": 3,
    "MAX_HOLD_DAYS_MOM": 1,
    "SECTOR_PENALTY_THRESHOLD": -0.05,
    "SECTOR_PENALTY_PTS": -2,
    "SECTOR_BONUS_PTS": 1,
}

# ─────────────────────────────────────────
# 활성 factor 적용
# ─────────────────────────────────────────
_active = _DAY if TRADE_FLAG == "DAY" else _SWING

INVEST_MIN_SCORE = _active["INVEST_MIN_SCORE"]
ATR_SL_MULT = _active["ATR_SL_MULT"]
ATR_TP_MULT = _active["ATR_TP_MULT"]
TP1_MULT = _active["TP1_MULT"]
TP1_RATIO = _active["TP1_RATIO"]
GAP_DOWN_LIMIT = _active["GAP_DOWN_LIMIT"]
MAX_HOLD_DAYS = _active["MAX_HOLD_DAYS"]
MAX_HOLD_DAYS_MIX = _active["MAX_HOLD_DAYS_MIX"]
MAX_HOLD_DAYS_MOM = _active["MAX_HOLD_DAYS_MOM"]
SECTOR_PENALTY_THRESHOLD = _active["SECTOR_PENALTY_THRESHOLD"]
SECTOR_PENALTY_PTS = _active["SECTOR_PENALTY_PTS"]
SECTOR_BONUS_PTS = _active["SECTOR_BONUS_PTS"]
