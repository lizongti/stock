if __name__ == '__main__':
    import sys
    from os.path import dirname, abspath
    sys.path.append(dirname(dirname(abspath(__file__))))

import datetime
from pandas import DataFrame
import presto
from tools import time
from retrying import retry


class MovingAverageController(presto.DataSource):
    _catalog = 'postgresql'
    _schema = 'stock'
    _table = 'moving_average'

    def __init__(self: object):
        super(MovingAverageController, self).__init__(
            MovingAverageController._catalog,
            MovingAverageController._schema,
            MovingAverageController._table,
        )

    def run(self: object, days: object = 0, **kargs):
        if len(kargs) > 0:
            start_date_object = time.to_date(kargs['start_date'])
            end_date_object = time.to_date(kargs['end_date'])

            for i in range((end_date_object - start_date_object).days + 1):
                date = time.date(start_date_object +
                                 datetime.timedelta(days=i))
                print('[%s][%s][%s]: updating..' %
                      (time.clock(), self, date), end='')
                self._update_by_date(date)
                print(' -> Done!')
        else:
            date = time.date(days)
            print('[%s][%s][%s]: updating..' %
                  (time.clock(), self, date), end='')
            self._update_by_date(date)
            print(' -> Done!')

    @retry(stop_max_attempt_number=100)
    def _update_by_date(self: object, date: str):
        print('.', end='')
        self._delete_by_date(date)
        df = self._select_by_date(date)
        self._insert_by_date(df)

    def _delete_by_date(self: object, date: str):
        presto.delete(self, {'date': date})

    def _insert_by_date(self: object, df: DataFrame):
        presto.insert(self, df)

    def _select_by_date(self: object, date: str) -> DataFrame:
        sql = """
                select * from (
                select code, max(date) as date, 
                (case when max(n) <= 5 then avg(close) else sum(case when n <= 5 then close else 0 end)/5 end) as ma5,
                (case when max(n) <= 10 then avg(close) else sum(case when n <= 10 then close else 0 end)/10 end) as ma10,
                (case when max(n) <= 20 then avg(close) else sum(case when n <= 20 then close else 0 end)/20 end) as ma20,
                (case when max(n) <= 30 then avg(close) else sum(case when n <= 30 then close else 0 end)/30 end) as ma30,
                (case when max(n) <= 40 then avg(close) else sum(case when n <= 40 then close else 0 end)/40 end) as ma40,
                (case when max(n) <= 60 then avg(close) else sum(case when n <= 60 then close else 0 end)/60 end) as ma60,
                (case when max(n) <= 120 then avg(close) else sum(case when n <= 120 then close else 0 end)/120 end) as ma120,
                (case when max(n) <= 200 then avg(close) else sum(case when n <= 200 then close else 0 end)/200 end) as ma200,
                (case when max(n) <= 240 then avg(close) else sum(case when n <= 240 then close else 0 end)/240 end) as ma240,
                (case when max(n) <= 250 then avg(close) else sum(case when n <= 250 then close else 0 end)/250 end) as ma250
                from
                (select code, date, close, row_number() over(partition by code order by date desc) as n from postgresql.stock.days where date <= '%s')
                where n <= 250
                group by code
                ) where date='%s'
            """ % (date, date)
        return presto.select(self, sql)


if __name__ == '__main__':
    if len(sys.argv) > 1:
        MovingAverageController().run(sys.argv[1])
    else:
        #MovingAverageController().run(start_date='2019-02-09', end_date='2021-09-30')
        MovingAverageController().run()
