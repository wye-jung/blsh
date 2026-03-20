import json
import logging
import shutil
import time
from pathlib import Path
from dataclasses import dataclass
from wye.blsh.common import dtutils, fileutils
from wye.blsh.common.env import DATA_DIR
from wye.blsh.domestic import _factor

log = logging.getLogger(__name__)

PO_DIR = DATA_DIR / "po"
PO_DONE_DIR = PO_DIR / "done"

def make_po_file(df):
    if not df.empty:
        po_list = df[
            [
                "ticker",
                "entry_date",
                "entry_price",
                "stop_loss",
                "take_profit",
                "atr",
                "atr_sl_mult",
                "atr_tp_mult",
                "expiry_date",
                "name",
                "mode",
                "max_hold_days",
            ]
        ].to_dict(orient="records")
        ctime = dtutils.ctime()
        po_file_name = f"po_{dtutils.now()}.json" if ctime < "150000" else get_final_po_name()
        fileutils.create_file(PO_DIR / po_file_name, po_list)

def get_final_po_name():
    return f"po_{dtutils.today()}_final.json"

def parse_po_file(path: Path) -> list[dict]:
    try:
        raw = json.loads(path.read_text())
        if isinstance(raw, dict):
            return [raw]
        if isinstance(raw, list):
            return raw
    except Exception as e:
        log.warning(f"po 파일 파싱 실패 ({path.name}): {e}")
    return []


def collect_po_orders(exclude_final: bool = True) -> dict[str, dict]:
    """PO_DIR에서 po_*.json 읽기 → ticker별 최신 주문. 처리 후 done으로 이동.

    [FIX] 파싱 실패 파일은 이동하지 않음 (다음 틱에서 재시도).
    """
    if not PO_DIR.exists():
        return {}
    today = dtutils.today()
    files = sorted(
        [
            f
            for f in PO_DIR.glob(f"po_{today}*.json")
            if not (exclude_final and f.name.endswith("final.json"))
        ],
        key=lambda f: f.stat().st_mtime,
    )
    if not files:
        return {}

    result: dict[str, dict] = {}

    for f in files:
        orders = parse_po_file(f)
        if not orders and f.stat().st_size > 0:
            # 파일이 비어있지 않은데 파싱 실패 → 쓰기 중일 수 있음, 이동 안 함
            log.info(f"  [po] 파싱 실패, 다음 틱 재시도: {f.name}")
            continue
        for o in orders:
            ticker = o.get("ticker")
            if ticker:
                result[ticker] = o
        move_po_file(f)

    return result


def move_po_file(path: Path):
    PO_DONE_DIR.mkdir(parents=True, exist_ok=True)
    dest = PO_DONE_DIR / path.name
    if dest.exists():
        dest = PO_DONE_DIR / f"{path.stem}_{int(time.time())}{path.suffix}"
    try:
        shutil.move(str(path), str(dest))
    except Exception as e:
        log.warning(f"po 파일 이동 실패 ({path.name}): {e}")


# ─────────────────────────────────────────
# 데이터 클래스
# ─────────────────────────────────────────
@dataclass
class Position:
    ticker: str
    name: str
    qty: int
    buy_price: float
    atr: float
    atr_sl_mult: float
    atr_tp_mult: float
    sl: float
    tp1: float
    tp2: float
    mode: str
    max_hold_days: int
    entry_date: str
    expiry_date: str = ""
    t1_done: bool = False
    qty_t1: int = 0
    realized_pnl: float = 0.0

class PositionLoader:
    def __init__(self, position_path: Path):
        self.position_path = position_path

    def load_positions(self) -> dict[str, Position]:
        if not self.position_path.exists():
            return {}
        try:
            data = json.loads(self.position_path.read_text())
            today = dtutils.today()
            valid: dict[str, Position] = {}
            for t, v in data.items():
                v.setdefault("realized_pnl", 0.0)
                v.setdefault("atr_sl_mult", _factor.ATR_SL_MULT)
                v.setdefault("atr_tp_mult", _factor.ATR_TP_MULT)
                v.setdefault("expiry_date", "")
                p = Position(**v)
                if p.max_hold_days == 0 and p.entry_date != today:
                    log.warning(f"  이전 데이 포지션 무시: {t} (entry={p.entry_date})")
                    continue
                # [FIX] 구버전 포지션 expiry_date 미설정 보정
                if not p.expiry_date and p.max_hold_days > 0:
                    try:
                        p.expiry_date = (
                            dtutils.add_biz_days(p.entry_date, p.max_hold_days)
                            or p.entry_date
                        )
                        log.info(
                            f"  expiry_date 보정: {t}  entry={p.entry_date}"
                            f"  +{p.max_hold_days}d → {p.expiry_date}"
                        )
                    except Exception as e:
                        log.warning(f"  expiry_date 보정 실패 ({t}): {e}")
                        p.expiry_date = today  # 안전 fallback: 오늘 청산 대상
                valid[t] = p
            return valid
        except Exception as e:
            log.warning(f"포지션 파일 로드 실패: {e}")
            return {}

    def save_positions(self, positions: dict[str, Position], swing_only: bool = False):
        to_save = (
            {t: p for t, p in positions.items() if p.max_hold_days > 0}
            if swing_only
            else positions
        )
        if to_save:
            fileutils.create_json(self.position_path, to_save)
        elif self.position_path.exists():
            self.position_path.unlink()