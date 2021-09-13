from apscheduler.schedulers.blocking import BlockingScheduler
from stocks import StocksDataSourceUpdater
from minutes import MinutesDataSourceUpdater

_scheduler = BlockingScheduler(timezone="Asia/Shanghai")


def _daily():
    StocksDataSourceUpdater().run()
    MinutesDataSourceUpdater().run()


if __name__ == "__main__":
    _scheduler.add_job(_daily, 'cron', hour=22, minute=0)
    _scheduler.start()
