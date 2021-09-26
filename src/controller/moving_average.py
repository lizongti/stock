import datetime
if __name__ == '__main__':
    import sys
    from os.path import dirname, abspath
    sys.path.append(dirname(dirname(abspath(__file__))))

from pandas.core.frame import DataFrame
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
            start_date = time.to_date(kargs['start_date'])
            end_date = time.to_date(kargs['end_date'])

            for i in range((end_date - start_date).days + 1):
                date = time.date(start_date + datetime.timedelta(days=i))
                print('[%s][%s][%s]: updating..' %
                      (time.clock(), self, date), end='')
                self._update_one_day(date)
                print(' -> Done!')
        else:
            date = time.date(days)
            print('[%s][%s][%s]: updating..' %
                  (time.clock(), self, date), end='')
            self._update_one_day(date)
            print(' -> Done!')

    @retry(stop_max_attempt_number=100)
    def _update_one_day(self: object, date: str):
        print('.', end='')
        self._delete_one_day(date)
        df = self._select_one_day(date)
        self._insert_one_day(df)

    def _delete_one_day(self: object, date: str):
        presto.delete(self, {'date': date})

    def _insert_one_day(self: object, df: DataFrame):
        presto.insert(self, df)

    def _select_one_day(self: object, date: str) -> DataFrame:
        sql = """
                select * from (
                select code, max(date) as date, 
                (case when max(n) <= 5 then avg(closing) else sum(case when n <= 5 then closing else 0 end)/5 end) as ma5,
                (case when max(n) <= 10 then avg(closing) else sum(case when n <= 10 then closing else 0 end)/10 end) as ma10,
                (case when max(n) <= 20 then avg(closing) else sum(case when n <= 20 then closing else 0 end)/20 end) as ma20,
                (case when max(n) <= 30 then avg(closing) else sum(case when n <= 30 then closing else 0 end)/30 end) as ma30,
                (case when max(n) <= 40 then avg(closing) else sum(case when n <= 40 then closing else 0 end)/40 end) as ma40,
                (case when max(n) <= 60 then avg(closing) else sum(case when n <= 60 then closing else 0 end)/60 end) as ma60,
                (case when max(n) <= 120 then avg(closing) else sum(case when n <= 120 then closing else 0 end)/120 end) as ma120,
                (case when max(n) <= 200 then avg(closing) else sum(case when n <= 200 then closing else 0 end)/200 end) as ma200,
                (case when max(n) <= 240 then avg(closing) else sum(case when n <= 240 then closing else 0 end)/240 end) as ma240,
                (case when max(n) <= 250 then avg(closing) else sum(case when n <= 250 then closing else 0 end)/250 end) as ma250
                from
                (select code, date, closing, row_number() over(partition by code order by date desc) as n from postgresql.stock.days where date <= '%s')
                where n <= 250
                group by code
                ) where date='%s'
            """ % (date, date)
        return presto.select(self, sql)


if __name__ == '__main__':
    if len(sys.argv) > 1:
        MovingAverageController().run(sys.argv[1])
    else:
        MovingAverageController().run(start_date='1990-12-19', end_date='2021-09-30')
