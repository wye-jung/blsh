import sys
from blsh.common import env, dtutils, fileutils
from blsh.wye.domestic import collector, scanner


def make_po(appendix=None):
    collector.collect_ohlcv()
    candidates = scanner.find_candidates(report=False)
    if not candidates.empty:
        candidates = candidates[
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
        ]
        po_json = candidates.to_json(orient="records", force_ascii=False)
        fileutils.create_file(env.PO_DIR, f"po_{appendix}.json", po_json)


if __name__ == "__main__":
    make_po(sys.argv[1] if len(sys.argv) > 1 else None)
