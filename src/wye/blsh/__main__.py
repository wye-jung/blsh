import logging
import sys
from wye.blsh.domestic import scanner, collector
from wye.blsh.domestic import trader_v2 as trader

log = logging.getLogger(__name__)

if __name__ == "__main__":
    if not sys.argv
        trader.run()
    elif sys.argv[1] == 'po':
        collector.collect_latest_ohlcv()
        scanner.issue_po()
    else:
        log.warning("invalid arguments")
        
