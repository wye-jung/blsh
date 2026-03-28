import logging
import sys
from wye.blsh.common import dtutils
from wye.blsh.database import query
from wye.blsh.domestic import scanner, collector
from wye.blsh.domestic import trader

log = logging.getLogger(__name__)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    if len(sys.argv) < 2:
        trader.run()
    elif sys.argv[1] == "po":
        collector.collect_holiday()
        today = dtutils.today()
        kh = query.get_krx_holiday(today)
        if kh is not None and kh["opnd_yn"] == "Y":
            collected, max_ohlcv_date = collector.collect()
            if collected:
                scanner.issue_po(max_ohlcv_date)
            else:
                log.warning(
                    f"최대 OHLCV 날짜 {max_ohlcv_date}가 오늘 {today} 또는 가장 가까운 영업일이 아닙니다."
                )

    else:
        log.warning("invalid arguments")
