from controller.days import DaysController
from tools.time import clock
from apscheduler.schedulers.blocking import BlockingScheduler
from controller import StocksController, MinutesController, DaysController


_scheduler = BlockingScheduler(timezone="Asia/Shanghai")


def _daily():
    StocksController().run()
    DaysController().run()
    MinutesController().run()


if __name__ == "__main__":
    print("[%s][scheduler] start!" % (clock()))
    _scheduler.add_job(_daily, 'cron', hour=18, minute=30)
    _scheduler.start()
