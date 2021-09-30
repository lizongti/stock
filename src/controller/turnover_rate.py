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
