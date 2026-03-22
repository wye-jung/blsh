from wye.blsh.kis.domestic_stock.domestic_stock_info import *  # noqa: F403
from wye.blsh.common.env import TEMP_DIR

get_kospi_info().to_csv(TEMP_DIR / "kospi.csv", index=False, encoding="cp949")
get_kosdaq_info().to_csv(TEMP_DIR / "kosdaq.csv", index=False, encoding="cp949")
