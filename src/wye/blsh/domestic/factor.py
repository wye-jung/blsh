"""
최적 파라미터 (20260328 기준, 최근 2년 백테스트)

파라미터              DAY                   SWING
──────────────────────────────────────────────────────
INVEST_MIN_SCORE     13      9
ATR_SL_MULT          3.0     2.5
ATR_TP_MULT          3.0     3.0
TP1_MULT             1.0     1.5
TP1_RATIO            0.3     0.3
GAP_DOWN_LIMIT       0.03    0.05
MAX_HOLD_DAYS(REV)   2       7
MAX_HOLD_DAYS_MIX    1       2
MAX_HOLD_DAYS_MOM    1       3
SECTOR_PENALTY       -3%/+0  -3%/-2
SECTOR_BONUS         +0      +1

[DAY]   2026-03-28 22:47  기간 20240328~20260328  282건  승률 8.5%  평균 +0.24%  총 +67.7%
[SWING] 2026-03-28 22:51  기간 20240328~20260328  10051건  승률 11.0%  평균 +0.09%  총 +886.8%

실행 후 grid_search 최적값으로 자동 갱신:
  uv run python -m wye.blsh.domestic.optimize.grid_search
"""

from wye.blsh.domestic import Factor
from wye.blsh.common.env import TRADE_FLAG

# ─────────────────────────────────────────
# 모드별 factor (grid_search 최적화 결과 반영)
# ─────────────────────────────────────────
# DAY 최적화: 2026-03-28 22:47  기간 20240328~20260328  282건  승률 8.5%  평균 +0.24%  총 +67.7%
_DAY = {
    "INVEST_MIN_SCORE": 13,
    "ATR_SL_MULT": 3.0,
    "ATR_TP_MULT": 3.0,
    "TP1_MULT": 1.0,  # 1차 익절: buy + ATR × TP1_MULT
    "TP1_RATIO": 0.3,  # 1차 익절 매도 비율 (1.0 = 전량)
    "GAP_DOWN_LIMIT": 0.03,
    "MAX_HOLD_DAYS": 2,
    "MAX_HOLD_DAYS_MIX": 1,
    "MAX_HOLD_DAYS_MOM": 1,
    "SECTOR_PENALTY_THRESHOLD": -0.03,  # 업종지수 MA20 대비 해당값 이하
    "SECTOR_PENALTY_PTS": 0,
    "SECTOR_BONUS_PTS": 0,  # 업종지수 MA20 이상일 때
}

# SWING 최적화: 2026-03-28 22:51  기간 20240328~20260328  10051건  승률 11.0%  평균 +0.09%  총 +886.8%
_SWING = {
    "INVEST_MIN_SCORE": 9,
    "ATR_SL_MULT": 2.5,
    "ATR_TP_MULT": 3.0,
    "TP1_MULT": 1.5,  # 1차 익절: buy + ATR × TP1_MULT
    "TP1_RATIO": 0.3,  # 1차 익절 매도 비율 (1.0 = 전량)
    "GAP_DOWN_LIMIT": 0.05,
    "MAX_HOLD_DAYS": 7,
    "MAX_HOLD_DAYS_MIX": 2,
    "MAX_HOLD_DAYS_MOM": 3,
    "SECTOR_PENALTY_THRESHOLD": -0.03,  # 업종지수 MA20 대비 해당값 이하
    "SECTOR_PENALTY_PTS": -2,
    "SECTOR_BONUS_PTS": 1,  # 업종지수 MA20 이상일 때
}

# ─────────────────────────────────────────
# 활성 factor 적용
# ─────────────────────────────────────────
active_factor = Factor(_DAY if TRADE_FLAG == "DAY" else _SWING)
