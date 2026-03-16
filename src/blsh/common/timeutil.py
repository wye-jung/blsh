from datetime import date, datetime

DEF_DATE_FMT="%Y%m%d"
DEF_TIME_FMT="%H%M%S"

def now(fmt:str=DEF_TIME_FMT) -> str:
    return datetime.now().strftime(fmt)

def today(fmt:str=DEF_DATE_FMT) -> str:
    return date.today().strftime(fmt)

def is_valid_date(date_str, date_format=DEF_DATE_FMT):
    try:
        datetime.strptime(date_str, date_format)
        return True
    except ValueError:
        return False



    
