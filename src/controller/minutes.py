if __name__ == '__main__':
    import sys
    from os.path import dirname, abspath
    sys.path.append(dirname(dirname(abspath(__file__))))

import datetime

from pandas.core.frame import DataFrame
import presto
from retrying import retry
from tools import time


class MinutesController(presto.DataSource):
    _catalog = 'hive'
    _schema = 'stock'
    _table = 'minutes'

    def __init__(self: object):
        super(MinutesController, self).__init__(
            MinutesController._catalog,
            MinutesController._schema,
            MinutesController._table,
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
        self._insert_by_date(date)

    def _delete_by_date(self: object, date: str):
        presto.delete(self, {'date': date})

    def _insert_by_date(self: object, date: str) -> DataFrame:
        sql = """
            insert into hive.stock.minutes select * from hive.stock.minutes_partitial where date='%s'
        """ % (date)
        presto.insert(self, sql)


if __name__ == '__main__':
    if len(sys.argv) > 1:
        MinutesController().run(sys.argv[1])
    else:
        MinutesController().run(-2)
