from datetime import date, datetime, timedelta
from wye.blsh.database import query

DEFAULT_DATE_FMT = "%Y%m%d"
DEFAULT_TIME_FMT = "%H%M%S"


def ctime(fmt: str = DEFAULT_TIME_FMT) -> str:
    return datetime.now().strftime(fmt)


def today(fmt: str = DEFAULT_DATE_FMT) -> str:
    return date.today().strftime(fmt)


def now(fmt: str = DEFAULT_DATE_FMT + DEFAULT_TIME_FMT) -> str:
    return datetime.now().strftime(fmt)


def is_valid_date(date_str, date_format=DEFAULT_DATE_FMT):
    try:
        datetime.strptime(date_str, date_format)
        return True
    except ValueError:
        return False


def strftime(time, fmt=DEFAULT_DATE_FMT):
    return time.strftime(fmt)


def next_biz_day(date_str=today(), fmt=DEFAULT_DATE_FMT):
    return add_biz_days(date_str, 1, fmt)


def add_days(date_str, days: int, fmt=DEFAULT_DATE_FMT):
    date_obj = datetime.strptime(date_str, fmt).date()
    return (date_obj + timedelta(days=days)).strftime(fmt)


def add_biz_days(date_str, days: int, fmt=DEFAULT_DATE_FMT):
    if days == 0:
        return date_str

    check_dt = add_days(date_str, days * 2, fmt)
    if query.get_krx_holiday(check_dt) is None:
        raise RuntimeError(f"krx_holiday에 {check_dt}까지 데이터가 없습니다.")

    rows = query.get_max_hold_dates(date_str, days)
    return rows[-1]["d"] if rows else None


def get_latest_biz_date():
    from wye.blsh.krx.krx_auth import login_krx
    from pykrx.website import krx

    login_krx()
    return krx.get_nearest_business_day_in_a_week()
