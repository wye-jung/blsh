"""
최적 파라미터 (2026-03-27 기준, 최근 2년 백테스트)

파라미터              DAY     SWING
──────────────────────────────────────
INVEST_MIN_SCORE     14      9
ATR_SL_MULT          2.0     2.5
ATR_TP_MULT          3.0     3.0
TP1_MULT             1.0     1.5
TP1_RATIO            0.3     0.3
GAP_DOWN_LIMIT       0.05    0.05
MAX_HOLD_DAYS(REV)   0       10
MAX_HOLD_DAYS_MIX    0       5
MAX_HOLD_DAYS_MOM    1       3
SECTOR_PENALTY       -5%/-2  -5%/-2
SECTOR_BONUS         +1      +1

실행 후 grid_search 최적값으로 갱신할 것:
  uv run python -m wye.blsh.domestic.optimize.grid_search
"""
from wye.blsh.common.env import TRADE_FLAG

# ─────────────────────────────────────────
# 모드별 factor (grid_search 최적화 결과 반영)
# ─────────────────────────────────────────
_DAY = {
    "INVEST_MIN_SCORE": 14,
    "ATR_SL_MULT": 2.0,
    "ATR_TP_MULT": 3.0,
    "TP1_MULT": 1.0,  # 1차 익절: buy + ATR × TP1_MULT
    "TP1_RATIO": 0.3,  # 1차 익절 매도 비율 (1.0 = 전량)
    "GAP_DOWN_LIMIT": 0.05,  # 갭하락 5% 이상 시 매수 스킵
    "MAX_HOLD_DAYS": 0,  # REV: 당일 청산
    "MAX_HOLD_DAYS_MIX": 0,
    "MAX_HOLD_DAYS_MOM": 1,  # MOM: 익일 청산 허용
    "SECTOR_PENALTY_THRESHOLD": -0.05,  # 업종지수 MA20 대비 -5% 이하
    "SECTOR_PENALTY_PTS": -2,
    "SECTOR_BONUS_PTS": 1,  # 업종지수 MA20 이상일 때 +1
}

_SWING = {
    "INVEST_MIN_SCORE": 9,
    "ATR_SL_MULT": 2.5,
    "ATR_TP_MULT": 3.0,
    "TP1_MULT": 1.5,
    "TP1_RATIO": 0.3,
    "GAP_DOWN_LIMIT": 0.05,
    "MAX_HOLD_DAYS": 10,
    "MAX_HOLD_DAYS_MIX": 5,
    "MAX_HOLD_DAYS_MOM": 3,
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


