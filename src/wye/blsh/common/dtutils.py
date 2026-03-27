from datetime import date, datetime, timedelta

_DATE_FMT = "%Y%m%d"
_TIME_FMT = "%H%M%S"

def ctime(fmt: str = _TIME_FMT) -> str:
    return datetime.now().strftime(fmt)

def today(fmt: str = _DATE_FMT) -> str:
    return date.today().strftime(fmt)

def add_time(time_str, fmt: str=_TIME_FMT, **kwargs):
    return (datetime.strptime(time_str, fmt) + timedelta(**kwargs)).strftime(fmt)

def add_days(date_str, days=0, fmt: str = _DATE_FMT):
    return (datetime.strptime(date_str, fmt) + timedelta(days=days)).strftime(fmt)

def add_biz_days(date_str, days: int, fmt=_DATE_FMT):
    if days == 0:
        return date_str

    from wye.blsh.database import query

    check_dt = add_days(date_str, days * 2, fmt)
    if query.get_krx_holiday(check_dt) is None:
        raise RuntimeError(f"krx_holiday에 {check_dt}까지 데이터가 없습니다.")

    rows = query.get_max_hold_dates(date_str, days)
    return rows[-1]["d"] if rows else None

def next_biz_date(date_str=None, fmt=_DATE_FMT):
    date_str = date_str if date_str else today()
    if fmt != _DATE_FMT:
        date_str = datetime.strptime(date_str, fmt).strftime(_DATE_FMT)

    from wye.blsh.database import query
    the_date = query.find_next_biz_date(date_str)
    if fmt != _DATE_FMT:
        the_date = datetime.strptime(the_date, _DATE_FMT).strftime(fmt)
    return the_date

def get_latest_biz_date():
    from wye.blsh.krx.krx_auth import login_krx
    from pykrx.website import krx

    login_krx()
    return krx.get_nearest_business_day_in_a_week()


if __name__ == "__main__":
    print(next_biz_date())

