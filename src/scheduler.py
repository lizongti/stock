import sys
from controller.days import DaysController
from controller.minutes_indicator import MinutesIndicatorController
from tools.time import clock
from apscheduler.schedulers.blocking import BlockingScheduler
from controller import *


_scheduler = BlockingScheduler(timezone="Asia/Shanghai")


def _daily():
    StocksController().run()
    DaysController().run()
    IndicatorController().run()
    MinutesController().run()
    QuantityRatioController().run()
    QuantityTrendController().run()
    MovingAverageController().run()
    TurnoverRatioController().run()
    MinutesIndicatorController().run()
    TurnController().run()


def _run(date: str):
    StocksController().run()
    DaysController().run(date)
    MinutesController().run(date)
    IndicatorController().run(date)
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
        print("[%s][scheduler] start!" % (clock()))
        _scheduler.add_job(_daily, 'cron', hour=15, minute=30)
        _scheduler.start()
