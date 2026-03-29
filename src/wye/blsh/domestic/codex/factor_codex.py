"""
Codex strategy factors optimized by grid_search_codex.

Optimization window
  - period: 20240327 ~ 20260327
  - updated: 2026-03-29 10:51

[DAY]   2026-03-29 10:48  period 20240327~20260327  2366 trades  win 3.6%  avg +0.04%  total +94.7%
[SWING] 2026-03-29 10:51  period 20240327~20260327  7927 trades  win 9.9%  avg +0.14%  total +1074.7%

Run again:
  uv run python -m wye.blsh.domestic.codex.grid_search_codex
"""

from wye.blsh.common.env import TRADE_FLAG

# DAY optimized: 2026-03-29 10:48  period 20240327~20260327  2366 trades  win 3.6%  avg +0.04%  total +94.7%
_DAY = {
    "INVEST_MIN_SCORE": 12,
    "AVG_TRDVAL_MIN": 5000000000,
    "RECENT_HIGH_DAYS": 30,
    "PULLBACK_MARGIN": 0.025,
    "ENTRY_ATR_MULT": 0,
    "ATR_SL_MULT": 1.6,
    "ATR_TP_MULT": 3.4,
    "TP1_MULT": 1,  # 1st partial take-profit ATR multiple
    "TP1_RATIO": 0.3,  # fraction to sell at TP1
    "MAX_HOLD_DAYS": 1,
    "MAX_HOLD_DAYS_MIX": 2,
    "MAX_HOLD_DAYS_MOM": 1,
    "MAX_CANDIDATES_PER_MARKET": 12,
}

# SWING optimized: 2026-03-29 10:51  period 20240327~20260327  7927 trades  win 9.9%  avg +0.14%  total +1074.7%
_SWING = {
    "INVEST_MIN_SCORE": 11,
    "AVG_TRDVAL_MIN": 2000000000,
    "RECENT_HIGH_DAYS": 90,
    "PULLBACK_MARGIN": 0.025,
    "ENTRY_ATR_MULT": 0.4,
    "ATR_SL_MULT": 2.6,
    "ATR_TP_MULT": 4.4,
    "TP1_MULT": 2,  # 1st partial take-profit ATR multiple
    "TP1_RATIO": 0.2,  # fraction to sell at TP1
    "MAX_HOLD_DAYS": 7,
    "MAX_HOLD_DAYS_MIX": 3,
    "MAX_HOLD_DAYS_MOM": 7,
    "MAX_CANDIDATES_PER_MARKET": 15,
}

DAY_FACTORS = _DAY
SWING_FACTORS = _SWING

_active = _DAY if TRADE_FLAG == "DAY" else _SWING

INVEST_MIN_SCORE = _active["INVEST_MIN_SCORE"]
AVG_TRDVAL_MIN = _active["AVG_TRDVAL_MIN"]
RECENT_HIGH_DAYS = _active["RECENT_HIGH_DAYS"]
PULLBACK_MARGIN = _active["PULLBACK_MARGIN"]
ENTRY_ATR_MULT = _active["ENTRY_ATR_MULT"]
ATR_SL_MULT = _active["ATR_SL_MULT"]
ATR_TP_MULT = _active["ATR_TP_MULT"]
TP1_MULT = _active["TP1_MULT"]
TP1_RATIO = _active["TP1_RATIO"]
MAX_HOLD_DAYS = _active["MAX_HOLD_DAYS"]
MAX_HOLD_DAYS_MIX = _active["MAX_HOLD_DAYS_MIX"]
MAX_HOLD_DAYS_MOM = _active["MAX_HOLD_DAYS_MOM"]
MAX_CANDIDATES_PER_MARKET = _active["MAX_CANDIDATES_PER_MARKET"]
