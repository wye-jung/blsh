from datetime import date, datetime, timedelta
from blsh.database import query

DEFAULT_DATE_FMT = "%Y%m%d"
DEFAULT_TIME_FMT = "%H%M%S"


def now(fmt: str = DEFAULT_TIME_FMT) -> str:
    return datetime.now().strftime(fmt)


def today(fmt: str = DEFAULT_DATE_FMT) -> str:
    return date.today().strftime(fmt)


def is_valid_date(date_str, date_format=DEFAULT_DATE_FMT):
    try:
        datetime.strptime(date_str, date_format)
        return True
    except ValueError:
        return False


def strftime(time, fmt=DEFAULT_DATE_FMT):
    return time.strftime(fmt)


def nextday(date_str, fmt=DEFAULT_DATE_FMT):
    add_days(date_str, 1)


def add_days(date_str, days: int, fmt=DEFAULT_DATE_FMT):
    date_obj = datetime.strptime(date_str, fmt).date()
    return (date_obj + timedelta(days=days)).strftime(fmt)


def add_biz_days(date_str, days: int, fmt=DEFAULT_DATE_FMT):
    dt = add_days(date_str, days, fmt)
    if query.get_krx_holiday(dt) is None:
        raise RuntimeError(f"krx_holiday에 {dt}가 없습니다.")
    if days == 0:
        return date_str
    else:
        rows = query.get_max_hold_dates(date_str, days)
        return rows[-1]["d"] if rows else None
