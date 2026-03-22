import json
import logging
import shutil
import time
from pathlib import Path
from wye.blsh.common import dtutils, fileutils
from wye.blsh.common.env import DATA_DIR
from wye.blsh.common import messageutils

log = logging.getLogger(__name__)

PO_DIR = DATA_DIR / "po"
PO_DONE_DIR = PO_DIR / "done"
PO_TYPE_PRE = "pre"
PO_TYPE_FIN = "final"
PO_TYPE_REG = "regular"


def make_po_file(df, po_type=None):
    """PO 파일 생성.

    Args:
        po_type: "pre" (전일 스캔 → 다음 영업일용),
                 "final" (오후 스캔 → 당일 청산 후 매수),
                 None (자동 판단: >=15:30 pre, >=14:00 final, 나머지 regular)
    """
    if df.empty:
        return

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

    entry_date = str(df.iloc[0]["entry_date"])
    today = dtutils.today()

    if po_type is None:
        ctime = dtutils.ctime()
        if entry_date > today or (entry_date == today and ctime < "080000"):
            po_type = PO_TYPE_PRE
        elif entry_date == today:
            if ctime >= "140000":
                po_type = PO_TYPE_FIN
            else:
                po_type = PO_TYPE_REG

    if po_type:
        po_file_name = f"po_{entry_date}_{po_type}.json"
        fileutils.create_json(PO_DIR / po_file_name, po_list)
        names = df["name"].to_list()
        log.info(f"[po] {po_file_name} 생성 ({len(po_list)}종목: {names})")
        messageutils.send_message(
            f"[po] {po_file_name} 생성 ({len(po_list)}종목: {names})"
        )
    else:
        log.warning(
            f"[po] po_type을 결정할 수 없습니다. ({len(po_list)}종목, entry_date={entry_date})"
        )


def get_pre_po_name():
    return f"po_{dtutils.today()}_{PO_TYPE_PRE}.json"


def get_final_po_name():
    return f"po_{dtutils.today()}_{PO_TYPE_FIN}.json"


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


def collect_po_orders(
    exclude_final: bool = True, exclude_pre: bool = True
) -> dict[str, dict]:
    """PO_DIR에서 po_*.json 읽기 → ticker별 최신 주문. 처리 후 done으로 이동.

    [FIX] 파싱 실패 파일은 이동하지 않음 (다음 틱에서 재시도).
    """
    if not PO_DIR.exists():
        return {}
    today = dtutils.today()
    files = sorted(
        [
            f
            for f in PO_DIR.glob(f"po_{today}_*.json")
            if not (exclude_final and f.name.endswith(f"_{PO_TYPE_FIN}.json"))
            and not (exclude_pre and f.name.endswith(f"_{PO_TYPE_PRE}.json"))
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
