"""A module for managing purchase orders and price tick calculations.

This module provides classes for handling purchase orders (`PO`) with functionality
such as creating orders, checking existence, and loading JSON data. It also includes
utility methods for price tick manipulations (`Tick`) and constants for time-based
milestones (`Milestone`).
"""

import logging
import json
import time
from typing import Final
from dataclasses import dataclass
from wye.blsh.common.env import DATA_DIR
from wye.blsh.common import dtutils, fileutils

log = logging.getLogger(__name__)

PO_TYPE_PRE: Final = "pre"
PO_TYPE_INI: Final = "ini"
PO_TYPE_FIN: Final = "fin"


class PO:
    _po_dir = DATA_DIR / "po"
    _done_dir = _po_dir / "done"

    def __init__(self, po_type, entry_date=None):
        self.po_type = po_type
        self.entry_date = entry_date if entry_date else dtutils.today()
        self.path = self._po_dir / f"po-{self.entry_date}-{po_type}.json"

    def exists(self):
        return self.path.exists()

    def create(self, orders: dict[str, dict]) -> bool:
        return fileutils.create_json(self.path, orders)

    def loads(self) -> dict[str, dict]:
        try:
            orders = json.loads(self.path.read_text())
            self._done_dir.mkdir(parents=True, exist_ok=True)
            dest = self._done_dir / self.path.name
            if dest.exists():
                dest = (
                    self._done_dir
                    / f"{self.path.stem}_{int(time.time())}{self.path.suffix}"
                )
            try:
                # shutil.move(str(self.path), str(dest))
                self.path.rename(dest)
            except Exception as e:
                log.warning(e)
        except Exception as e:
            log.warning(e)
            orders = {}

        return orders


class Tick:
    @staticmethod
    def _tick_size(price: float) -> int:
        if price < 1_000:
            return 1
        elif price < 5_000:
            return 5
        elif price < 10_000:
            return 10
        elif price < 50_000:
            return 50
        elif price < 100_000:
            return 100
        elif price < 500_000:
            return 500
        else:
            return 1_000

    @classmethod
    def floor_tick(cls, price: float) -> int:
        """가격을 호가 단위 이하로 내림 (SL 등 하한 기준에 사용)"""
        if price <= 0:
            return 0
        tick = cls._tick_size(price)
        return int(price) // tick * tick

    @classmethod
    def ceil_tick(cls, price: float) -> int:
        """가격을 호가 단위 이상으로 올림 (TP 등 상한 기준에 사용)"""
        if price <= 0:
            return 0
        tick = cls._tick_size(price)
        floored = int(price) // tick * tick
        result = floored if floored >= price else floored + tick
        # 올림 결과가 더 높은 tick 구간으로 넘어간 경우 재보정
        final_tick = cls._tick_size(result)
        if result % final_tick != 0:
            result = (result // final_tick + 1) * final_tick
        return result


class Milestone:
    NXT_OPEN_TIME: Final = "080000"  # NXT 프리마켓 개장 (매수 SOR 가능)
    KRX_OPEN_TIME: Final = "090000"  # KRX 정규장 개장 (매도 가능)
    KRX_EARLY_TIME: Final = "101500"  # 장 초반 매수
    LIQUIDATE_TIME: Final = "151500"  # 청산시간
    KRX_CLOSE_TIME: Final = "153000"  # KRX 마감
    NXT_CLOSE_TIME: Final = "200000"  # NTX 마감


@dataclass
class Factor:
    invest_min_score: int
    atr_sl_mult: float
    atr_tp_mult: float
    tp1_mult: float
    tp1_ratio: float
    gap_down_limit: float
    max_hold_days: int
    max_hold_days_mix: int
    max_hold_days_mom: int
    sector_penalty_threshold: float
    sector_penalty_pts: int
    sector_bonus_pts: int

    def __init__(self, d: dict):
        self.invest_min_score = d["INVEST_MIN_SCORE"]
        self.atr_sl_mult = d["ATR_SL_MULT"]
        self.atr_tp_mult = d["ATR_TP_MULT"]
        self.tp1_mult = d["TP1_MULT"]
        self.tp1_ratio = d["TP1_RATIO"]
        self.gap_down_limit = d["GAP_DOWN_LIMIT"]
        self.max_hold_days = d["MAX_HOLD_DAYS"]
        self.max_hold_days_mix = d["MAX_HOLD_DAYS_MIX"]
        self.max_hold_days_mom = d["MAX_HOLD_DAYS_MOM"]
        self.sector_penalty_threshold = d["SECTOR_PENALTY_THRESHOLD"]
        self.sector_penalty_pts = d["SECTOR_PENALTY_PTS"]
        self.sector_bonus_pts = d["SECTOR_BONUS_PTS"]
