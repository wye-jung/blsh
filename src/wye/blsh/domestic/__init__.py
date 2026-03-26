import json
from pathlib import Path
from wye.blsh.common.env import DATA_DIR
from wye.blsh.common import dtutils

PO_DIR = DATA_DIR / "po"
PO_DONE_DIR = PO_DIR / "done"
PO_TYPE_PRE = "pre"
PO_TYPE_INI = "ini"
PO_TYPE_FIN = "fin"

def get_po_path(po_type, entry_date=None):
    entry_date = entry_date if entry_date else dtutils.today()
    return PO_DIR / f"po-{entry_date}-{po_type}.json"


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

    @staticmethod
    def floor_tick(price: float) -> int:
        """가격을 호가 단위 이하로 내림 (SL 등 하한 기준에 사용)"""
        tick = Tick._tick_size(price)
        return int(price) // tick * tick

    @staticmethod
    def ceil_tick(price: float) -> int:
        """가격을 호가 단위 이상으로 올림 (TP 등 상한 기준에 사용)"""
        tick = Tick._tick_size(price)
        floored = int(price) // tick * tick
        result = floored if floored >= price else floored + tick
        # 올림 결과가 더 높은 tick 구간으로 넘어간 경우 재보정
        final_tick = Tick._tick_size(result)
        if result % final_tick != 0:
            result = (result // final_tick + 1) * final_tick
        return result