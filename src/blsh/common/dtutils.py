from datetime import date, datetime, timedelta

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
    date_obj = datetime.strptime(date_str, fmt).date()
    return (date_obj + timedelta(days=1)).strftime(fmt)
