import logging
import sys
from wye.blsh.common import dtutils
from wye.blsh.database import query
from wye.blsh.domestic import _po, scanner, collector
from wye.blsh.domestic import trader_v2 as trader

log = logging.getLogger(__name__)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        trader.run()
    elif sys.argv[1] == "po":
        collector.collect_holiday()
        today = dtutils.today()
        kh = query.get_krx_holiday(today)
        if kh is not None and kh["opnd_yn"] == "Y":
            max_ohlcv_date = collector.collect()
            if (
                max_ohlcv_date == today
                or max_ohlcv_date == dtutils.get_latest_biz_date()
            ):
                df = scanner.issue_po(max_ohlcv_date)

    else:
        log.warning("invalid arguments")
