import json
from pathlib import Path
from wye.blsh.common.env import DATA_DIR
from wye.blsh.common import dtutils, fileutils

PO_TYPE_PRE = "pre"
PO_TYPE_INI = "ini"
PO_TYPE_FIN = "fin"

class PO:
    _po_dir = DATA_DIR / "po"
    _done_dir = _po_dir / "done"

    def __init__(self, po_type, entry_date=None):
        self.po_type = po_type
        self.entry_date = entry_date if entry_date else dtutils.today()
        self path = self._po_dir / "po" / f"po-{entry_date}-{po_type}.json"

    def exists(self):
        return self.path.exists()

    def create(self, orders:dict[str, dict])->bool:
        return fileutils.create_json(self.path, orders):

    def loads(self)->dict[str, dict]:
        try:
            orders = json.loads(self.path.read_text())
            self._done_dir.mkdir(parents=True, exist_ok=True)
            dest = self._done_dir / self.path.name
            if dest.exists():
                dest = self._done_dir / f"{self.path.stem}_{int(time.time())}{self.path.suffix}"
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
        tick = cls._tick_size(price)
        return int(price) // tick * tick

    @classmethod
    def ceil_tick(cls, price: float) -> int:
        """가격을 호가 단위 이상으로 올림 (TP 등 상한 기준에 사용)"""
        tick = cls._tick_size(price)
        floored = int(price) // tick * tick
        result = floored if floored >= price else floored + tick
        # 올림 결과가 더 높은 tick 구간으로 넘어간 경우 재보정
        final_tick = cls._tick_size(result)
        if result % final_tick != 0:
            result = (result // final_tick + 1) * final_tick
        return result