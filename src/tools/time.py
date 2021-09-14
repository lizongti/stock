import datetime


def clock() -> str:
    return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
