if __name__ == '__main__':
    import sys
    from os.path import dirname, abspath
    sys.path.append(dirname(dirname(abspath(__file__))))

import datetime
from pandas.core.frame import DataFrame
import presto
from tools import time
from retrying import retry


class TurnoverRatioController(presto.DataSource):
    _catalog = 'postgresql'
    _schema = 'stock'
    _table = 'turnover_ratio'
    _columns = ['ratio', 'date', 'code']

    def __init__(self: object):
        super(TurnoverRatioController, self).__init__(
            TurnoverRatioController._catalog,
            TurnoverRatioController._schema,
            TurnoverRatioController._table,
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
            select days.code, days.date, (days.turnover/indicator.total_mv)*100 as ratio from
            (select code, date, turnover from postgresql.stock.days where date='%s') days,
            (select code, total_mv from postgresql.stock.indicator where date='%s') indicator
            where days.code=indicator.code
        """ % (date, date)
        return presto.select(self, sql)


if __name__ == '__main__':
    if len(sys.argv) > 1:
        TurnoverRatioController().run(sys.argv[1])
    else:
        #TurnoverRatioController().run(start_date='1990-12-19', end_date='2021-09-29')
        TurnoverRatioController().run(-1)
