from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

_KST = ZoneInfo("Asia/Seoul")

DATE_FMT = "%Y%m%d"
TIME_FMT = "%H%M%S"


def ctime(fmt: str = TIME_FMT) -> str:
    return datetime.now(_KST).strftime(fmt)


def today(fmt: str = DATE_FMT) -> str:
    return datetime.now(_KST).date().strftime(fmt)


def add_time(time_str, fmt: str = TIME_FMT, **kwargs):
    return (datetime.strptime(time_str, fmt) + timedelta(**kwargs)).strftime(fmt)


def add_days(date_str=None, days=0, fmt: str = DATE_FMT):
    date_str = date_str if date_str else today(fmt)
    return (datetime.strptime(date_str, fmt) + timedelta(days=days)).strftime(fmt)


def add_biz_days(date_str, days: int, fmt=DATE_FMT) -> str | None:
    """date_str 기준 N영업일 후 날짜 반환.

    Args:
        days: 양수만 지원 (미래 영업일). 0이면 date_str 그대로 반환.

    Returns:
        YYYYMMDD 문자열 또는 None (데이터 부족 시).
    """
    if days == 0:
        return date_str
    if days < 0:
        raise ValueError(f"add_biz_days: 음수 days 미지원 (days={days})")

    from wye.blsh.database import query

    # 연휴(추석/설 최대 9일) 감안하여 days*3으로 여유있게 검증
    check_dt = add_days(date_str, days * 3, fmt)
    if query.get_max_ohlcv_date() >= check_dt:
        rows = query.get_max_hold_dates_from_ohlcv(date_str, days)
    else:
        if query.get_krx_holiday(check_dt) is None:
            raise RuntimeError(f"krx_holiday에 {check_dt}까지 데이터가 없습니다.")

        rows = query.get_max_hold_dates(date_str, days)

    if not rows:
        return None
    return rows[-1]["d"]


def next_biz_date(date_str=None, fmt=DATE_FMT):
    date_str = date_str if date_str else today()
    if fmt != DATE_FMT:
        date_str = datetime.strptime(date_str, fmt).strftime(DATE_FMT)

    from wye.blsh.database import query

    the_date = query.find_next_biz_date(date_str)
    if the_date and fmt != DATE_FMT:
        the_date = datetime.strptime(the_date, DATE_FMT).strftime(fmt)
    return the_date


def prev_biz_date(date_str=None, fmt=DATE_FMT):
    date_str = date_str if date_str else today()
    if fmt != DATE_FMT:
        date_str = datetime.strptime(date_str, fmt).strftime(DATE_FMT)

    from wye.blsh.database import query

    the_date = query.find_prev_biz_date(date_str)
    if the_date and fmt != DATE_FMT:
        the_date = datetime.strptime(the_date, DATE_FMT).strftime(fmt)
    return the_date


def max_ohlcv_date():
    from wye.blsh.database import query

    return query.get_max_ohlcv_date()


def get_latest_biz_date():
    from wye.blsh.krx.krx_auth import login_krx
    from pykrx.website import krx

    login_krx()
    return krx.get_nearest_business_day_in_a_week()


if __name__ == "__main__":
    print(next_biz_date())
