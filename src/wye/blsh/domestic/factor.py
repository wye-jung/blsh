"""
최적 파라미터 (20260329 기준, 최근 1년 백테스트)
2026-03-29 23:56  기간 20250329~20260329  493건  승률 30.0%  평균 +0.10%  총 +50.7%

command:
  uv run python -m wye.blsh.domestic.optimize.grid_search
"""

INVEST_MIN_SCORE = 11
ATR_SL_MULT = 3.0
ATR_TP_MULT = 3.0
TP1_MULT = 1.5  # 1차 익절: buy + ATR × TP1_MULT
TP1_RATIO = 0.7  # 1차 익절 매도 비율 (1.0 = 전량)
GAP_DOWN_LIMIT = 0.03
MAX_HOLD_DAYS = 3
MAX_HOLD_DAYS_MIX = 2
MAX_HOLD_DAYS_MOM = 3
SECTOR_PENALTY_THRESHOLD = -0.05  # 업종지수 MA20 대비 해당값 이하
SECTOR_PENALTY_PTS = -2
SECTOR_BONUS_PTS = 0  # 업종지수 MA20 이상일 때
