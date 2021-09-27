import sys
from controller.days import DaysController
from tools.time import clock
from apscheduler.schedulers.blocking import BlockingScheduler
from controller import *


_scheduler = BlockingScheduler(timezone="Asia/Shanghai")


def _daily():
    StocksController().run()
    DaysController().run()
    MinutesController().run()
    QuantityRatioController().run()
    MovingAverageController().run()
    TurnController().run()


def _run(date: str):
    StocksController().run()
    DaysController().run(date)
    MinutesController().run(date)
    QuantityRatioController().run(date)
    MovingAverageController().run(date)
    TurnController().run(date)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        _run(sys.argv[1])
    else:
        print("[%s][scheduler] start!" % (clock()))
        _scheduler.add_job(_daily, 'cron', hour=15, minute=5)
        _scheduler.start()
