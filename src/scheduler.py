import sys
from controller.days import DaysController
from controller.minutes_indicator import MinutesIndicatorController
from tools import time
from apscheduler.schedulers.blocking import BlockingScheduler
from controller import *


_scheduler = BlockingScheduler(timezone="Asia/Shanghai")


def _daily():
    date = time.date()
    _run(date)


def _run(date):
    _level0(date)
    _level1(date)


def _level0(date):
    StocksController().run()
    DaysController().run(date)
    MinutesController().run(date)
    IndicatorController().run(date)


def _level1(date: str):
    QuantityRatioController().run(date)
    QuantityTrendController().run(date)
    MovingAverageController().run(date)
    TurnoverRatioController().run(date)
    MinutesIndicatorController().run(date)
    TurnController().run(date)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        _run(sys.argv[1])
    else:
        print("[%s][scheduler] start!" % (time.clock()))
        _scheduler.add_job(_daily, 'cron', hour=15, minute=30)
        _scheduler.start()




