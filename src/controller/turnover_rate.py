if __name__ == '__main__':
    import sys
    from os.path import dirname, abspath
    sys.path.append(dirname(dirname(abspath(__file__))))

import datetime
from pandas.core.frame import DataFrame
import presto
from tools import time
from retrying import retry


class TurnoverRateController(presto.DataSource):
    _catalog = 'postgresql'
    _schema = 'stock'
    _table = 'turnover_rate'

    def __init__(self: object):
        super(TurnoverRateController, self).__init__(
            TurnoverRateController._catalog,
            TurnoverRateController._schema,
            TurnoverRateController._table,
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
            select day.code, day.date, (case average.turnover when 0 then 1 else day.turnover/average.turnover end) as rate from
            (select code, avg(turnover) as turnover from
            (select code, turnover, row_number() over(partition by code order by date desc) as n from postgresql.stock.days where date <= '%s')
            where n >= 2 and n <= 6
            group by code) average,
            (select code, date, turnover from
            (select code, date, turnover, row_number() over(partition by code order by date desc) as n from postgresql.stock.days where date <= '%s')
            where n=1) day
            where average.code = day.code and day.date='%s'
            """ % (date, date, date)
        return presto.select(self, sql)


if __name__ == '__main__':
    if len(sys.argv) > 1:
        TurnoverRateController().run(sys.argv[1])
    else:
        # QuantityRatioController().run(start_date='2010-08-06', end_date='2021-09-30')
        TurnoverRateController().run()
