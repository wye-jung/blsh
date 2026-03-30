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
from wye.blsh.common.env import DATA_DIR
from wye.blsh.common import dtutils, fileutils

log = logging.getLogger(__name__)

PO_TYPE_PRE: Final = "pre"
PO_TYPE_INI: Final = "ini"
PO_TYPE_FIN: Final = "fin"


class PO:
    def __init__(self, po_type, entry_date=None):
        from wye.blsh.common.env import KIS_ENV
        _po_dir = DATA_DIR / "po" / KIS_ENV  # demo/real 환경 분리
        _po_dir.mkdir(parents=True, exist_ok=True)
        self._done_dir = _po_dir / "done"
        self.po_type = po_type
        self.entry_date = entry_date if entry_date else dtutils.today()
        self.path = _po_dir / f"po-{self.entry_date}-{po_type}.json"

    def exists(self):
        return self.path.exists()

    def create(self, orders: dict[str, dict]) -> bool:
        return fileutils.create_json(self.path, orders)

    def loads(self) -> dict[str, dict]:
        try:
            text = self.path.read_text()
        except FileNotFoundError:
            return {}
        try:
            orders = json.loads(text)
        except json.JSONDecodeError as e:
            log.error(f"PO 파일 손상 ({self.path}): {e}")
            return {}

        self._done_dir.mkdir(parents=True, exist_ok=True)
        dest = self._done_dir / self.path.name
        if dest.exists():
            dest = (
                self._done_dir
                / f"{self.path.stem}_{int(time.time())}{self.path.suffix}"
            )
        try:
            self.path.rename(dest)
        except Exception as e:
            log.warning(e)
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
    NXT_CLOSE_TIME: Final = "200000"  # NXT 마감
