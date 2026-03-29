"""
최적 파라미터 (20260328 기준, 최근 2년 백테스트)
2026-03-28 22:51  기간 20240328~20260328  10051건  승률 11.0%  평균 +0.09%  총 +886.8%

command:
  uv run python -m wye.blsh.domestic.optimize.grid_search
"""

INVEST_MIN_SCORE = 9
ATR_SL_MULT = 2.5
ATR_TP_MULT = 3.0
TP1_MULT = 1.5  # 1차 익절: buy + ATR × TP1_MULT
TP1_RATIO = 0.3  # 1차 익절 매도 비율 (1.0 = 전량)
GAP_DOWN_LIMIT = 0.05
MAX_HOLD_DAYS = 7
MAX_HOLD_DAYS_MIX = 2
MAX_HOLD_DAYS_MOM = 3
SECTOR_PENALTY_THRESHOLD = -0.03  # 업종지수 MA20 대비 해당값 이하
SECTOR_PENALTY_PTS = -2
SECTOR_BONUS_PTS = 1  # 업종지수 MA20 이상일 때
