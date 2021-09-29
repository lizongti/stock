import datetime
from . import math


def clock() -> str:
    return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')


def date(days: object = None) -> str:
    if days is None:
        return datetime.datetime.now().strftime('%Y-%m-%d')
    elif isinstance(days, str) and not math.is_int(days):
        return days
    elif isinstance(days, datetime.date) or isinstance(days, datetime.datetime):
        return days.strftime("%Y-%m-%d")
    else:
        return (datetime.datetime.now() + datetime.timedelta(days=int(days))).strftime('%Y-%m-%d')


def to_date(date: str) -> datetime.date:
    return datetime.datetime.strptime(date, "%Y-%m-%d")
