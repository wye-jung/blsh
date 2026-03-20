import sys
from blsh.common.env import DATA_DIR
from blsh.common import dtutils, fileutils
from blsh.wye.domestic import collector, scanner

PO_DIR = DATA_DIR / "po"
PO_DONE_DIR = PO_DIR / "done"

def _make_po_file(day_final:bool=False):
    collector.collect_ohlcv()
    candidates = scanner.find_candidates(report=False)
    if not candidates.empty:
        po_list = candidates[
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
        appendix = f"{dtutils.today()}_final" if day_final else dtutils.now() 
        po_file = PO_DIR / f"po_{appendix}.json"
        fileutils.create_file(po_file, po_list)

def _parse_po_file(path: Path) -> list[dict]:
    try:
        raw = json.loads(path.read_text())
        if isinstance(raw, dict):
            return [raw]
        if isinstance(raw, list):
            return raw
    except Exception as e:
        log.warning(f"po 파일 파싱 실패 ({path.name}): {e}")
    return []


def _collect_po_orders(exclude_after_liquidate: bool = True) -> dict[str, dict]:
    """PO_DIR에서 po_*.json 읽기 → ticker별 최신 주문. 처리 후 done으로 이동.

    [FIX] 파싱 실패 파일은 이동하지 않음 (다음 틱에서 재시도).
    po.py의 비원자적 쓰기 중 읽기 시 partial JSON 대응.
    """
    if not PO_DIR.exists():
        return {}

    files = sorted(
        [
            f
            for f in PO_DIR.glob("po_*.json")
            if not (exclude_after_liquidate and f.name == PO_AFTER_LIQUIDATE)
        ],
        key=lambda f: f.stat().st_mtime,
    )
    if not files:
        return {}

    result: dict[str, dict] = {}

    for f in files:
        orders = _parse_po_file(f)
        if not orders and f.stat().st_size > 0:
            # 파일이 비어있지 않은데 파싱 실패 → 쓰기 중일 수 있음, 이동 안 함
            log.info(f"  [po] 파싱 실패, 다음 틱 재시도: {f.name}")
            continue
        for o in orders:
            ticker = o.get("ticker")
            if ticker:
                result[ticker] = o
        move_po_file_to_done(f)

    return result


def _move_po_file_to_done(path: Path):
    PO_DONE_DIR.mkdir(parents=True, exist_ok=True)
    dest = PO_DONE_DIR / path.name
    if dest.exists():
        dest = PO_DONE_DIR / f"{path.stem}_{int(time.time())}{path.suffix}"
    try:
        shutil.move(str(path), str(dest))
    except Exception as e:
        log.warning(f"po 파일 이동 실패 ({path.name}): {e}")

if __name__ == "__main__":
    _make_po_file()
