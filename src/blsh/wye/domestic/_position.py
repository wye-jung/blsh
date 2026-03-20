import logging

from dataclasses import asdict, dataclass
from blsh.common import fileutils
from blsh.wye.domestic._tick import floor_tick as _floor_tick, ceil_tick as _ceil_tick

log = logging.getLogger(__name__)

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

def make_position(
    c: dict, buy_price: float, qty: int, entry_date: str, expiry_date: str = ""
) -> Position:
    """po.json dict → Position 생성."""
    atr = float(c["atr"])
    atr_sl_mult = float(
        c["atr_sl_mult"] if c.get("atr_sl_mult") is not None else fac.ATR_SL_MULT
    )
    atr_tp_mult = float(
        c["atr_tp_mult"] if c.get("atr_tp_mult") is not None else fac.ATR_TP_MULT
    )

    if c.get("max_hold_days") is not None:
        max_hold = int(c["max_hold_days"])
    elif expiry_date and expiry_date > entry_date:
        max_hold = 1
    else:
        max_hold = 0

    if not expiry_date:
        try:
            expiry_date = dtutils.add_biz_days(entry_date, max_hold) or entry_date
        except Exception as e:
            log.warning(f"  expiry_date 계산 실패 ({c.get('ticker')}): {e}")
            expiry_date = entry_date

    sl = _floor_tick(buy_price - atr_sl_mult * atr)
    tp1 = _ceil_tick(buy_price + TP1_MULT * atr)
    tp2 = _ceil_tick(buy_price + atr_tp_mult * atr)
    qty_t1 = max(1, qty // 2)
    if qty < 2:
        log.warning(
            f"  {c['ticker']} 수량={qty} → 1차 익절 시 전량 청산, 2차 익절 없음"
        )

    return Position(
        ticker=c["ticker"],
        name=c.get("name", c["ticker"]),
        qty=qty,
        buy_price=buy_price,
        atr=atr,
        atr_sl_mult=atr_sl_mult,
        atr_tp_mult=atr_tp_mult,
        sl=sl,
        tp1=tp1,
        tp2=tp2,
        mode=c.get("mode", "REV"),
        max_hold_days=max_hold,
        entry_date=entry_date,
        expiry_date=expiry_date,
        t1_done=False,
        qty_t1=qty_t1,
    )


# ─────────────────────────────────────────
# 포지션 영속화
# ─────────────────────────────────────────
def load_positions() -> dict[str, Position]:
    if not POSITIONS_FILE.exists():
        return {}
    try:
        data = json.loads(POSITIONS_FILE.read_text())
        today = dtutils.today()
        valid: dict[str, Position] = {}
        for t, v in data.items():
            v.setdefault("realized_pnl", 0.0)
            v.setdefault("atr_sl_mult", fac.ATR_SL_MULT)
            v.setdefault("atr_tp_mult", fac.ATR_TP_MULT)
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


def save_positions(positions: dict[str, Position], swing_only: bool = False):
    to_save = (
        {t: p for t, p in positions.items() if p.max_hold_days > 0}
        if swing_only
        else positions
    )
    POSITIONS_FILE.parent.mkdir(parents=True, exist_ok=True)
    if to_save:
        tmp = POSITIONS_FILE.with_suffix(".tmp")
        try:
            tmp.write_text(
                json.dumps(
                    {t: asdict(p) for t, p in to_save.items()},
                    ensure_ascii=False,
                    indent=2,
                )
            )
            tmp.replace(POSITIONS_FILE)
        except Exception as e:
            log.error(f"포지션 저장 실패: {e}")
            tmp.unlink(missing_ok=True)
    elif POSITIONS_FILE.exists():
        POSITIONS_FILE.unlink()