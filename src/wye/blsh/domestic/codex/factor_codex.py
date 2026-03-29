"""
scanner_codex / simulator_codex 최적 파라미터

최적화 대상
  - scanner_codex 종목 선정 임계값
  - trader.py 와 동일하게 사용되는 ATR / TP1 / 보유기간 파라미터

최적화 스크립트
  uv run python -m wye.blsh.domestic.codex.grid_search_codex
"""

from wye.blsh.common.env import TRADE_FLAG

# DAY 기본값
_DAY = {
    "INVEST_MIN_SCORE": 9,
    "AVG_TRDVAL_MIN": 2_000_000_000,
    "RECENT_HIGH_DAYS": 20,
    "PULLBACK_MARGIN": 0.015,
    "ENTRY_ATR_MULT": 0.15,
    "ATR_SL_MULT": 1.6,
    "ATR_TP_MULT": 2.4,
    "TP1_MULT": 1.0,
    "TP1_RATIO": 0.5,
    "MAX_HOLD_DAYS": 0,
    "MAX_HOLD_DAYS_MIX": 1,
    "MAX_HOLD_DAYS_MOM": 1,
    "MAX_CANDIDATES_PER_MARKET": 8,
}

# SWING 기본값
_SWING = {
    "INVEST_MIN_SCORE": 8,
    "AVG_TRDVAL_MIN": 1_000_000_000,
    "RECENT_HIGH_DAYS": 55,
    "PULLBACK_MARGIN": 0.025,
    "ENTRY_ATR_MULT": 0.30,
    "ATR_SL_MULT": 2.2,
    "ATR_TP_MULT": 3.2,
    "TP1_MULT": 1.5,
    "TP1_RATIO": 0.3,
    "MAX_HOLD_DAYS": 7,
    "MAX_HOLD_DAYS_MIX": 5,
    "MAX_HOLD_DAYS_MOM": 3,
    "MAX_CANDIDATES_PER_MARKET": 12,
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
