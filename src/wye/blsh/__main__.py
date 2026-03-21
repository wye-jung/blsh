import logging
import sys
from wye.blsh.domestic import scanner, collector
from wye.blsh.domestic import trader_v2 as trader

log = logging.getLogger(__name__)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        trader.run()
    elif sys.argv[1] == "po":
        # uv run python -m wye.blsh po          → 자동 판단 (>=15:30 pre, >=14:00 final, else regular)
        # uv run python -m wye.blsh po pre      → 전일 스캔 (다음 영업일용)
        # uv run python -m wye.blsh po final    → 오후 스캔 (청산 후 매수)
        po_type = sys.argv[2] if len(sys.argv) > 2 else None
        collector.collect_latest_ohlcv()
        scanner.issue_po(po_type=po_type)
    else:
        log.warning("invalid arguments")
