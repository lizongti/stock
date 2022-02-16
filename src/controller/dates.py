if __name__ == '__main__':
    import sys
    from os.path import dirname, abspath
    sys.path.append(dirname(dirname(abspath(__file__))))

import datetime
from pandas import DataFrame
import akshare
import presto
from retrying import retry
from tools import time


class DatesController(presto.DataSource):
    _catalog = 'redis'
    _schema = 'stock'
    _table = 'dates'
    _columns = ['key', 'date', 'open']
    _rename_columns = ['date', 'open', 'close', 'high', 'low', 'turnover', 'volume',
                       'amplitude', 'change_rate', 'change', 'turnover_rate']

    def __init__(self: object):
        super(DatesController, self).__init__(
            DatesController._catalog,
            DatesController._schema,
            DatesController._table,
        )

    def run(self: object, days: object = 0, **kargs):
        if len(kargs) > 0:
            start_date = kargs['start_date']
            end_date = kargs['end_date']
            print('[%s][%s][%s..%s]: updating..' %
                  (time.clock(), self, start_date, end_date), end='')
            self._update_by_dates(start_date, end_date)
            print(' -> Done!')
        else:
            date = time.date(days)
            print('[%s][%s][%s]: updating..' %
                  (time.clock(), self, date), end='')
            self._update_by_date(date)
            print(' -> Done!')

    def is_open(self: object, days: str) -> bool:
        date = time.date(days)
        return presto.select(self, {'key': date}).iloc[0]['open'] == "1"

    @retry(stop_max_attempt_number=100)
    def _update_by_dates(self: object, start_date: str, end_date: str):
        print('.', end='')
        df = akshare.stock_zh_a_hist(
            symbol="000001", start_date=start_date, end_date=end_date, adjust="qfq")
        df.columns = DatesController._rename_columns
        start_date_object = time.to_date(start_date)
        end_date_object = time.to_date(end_date)
        data = []
        for i in range((end_date_object - start_date_object).days + 1):
            date = time.date(start_date_object +
                             datetime.timedelta(days=i))
            if df.query('date=="%s"' % (date)).shape[0] == 0:
                list = [date, date, "0"]
            else:
                list = [date, date, "1"]

            data.append(list)

        df = DataFrame(data=data, columns=DatesController._columns)
        presto.insert(self, df)

    @retry(stop_max_attempt_number=100)
    def _update_by_date(self: object, date: str):
        print('.', end='')
        df = akshare.stock_zh_a_hist(
            symbol="000001", start_date=date, end_date=date, adjust="qfq")
        data = []
        if df.shape[0] == 0:
            list = [date, date, "0"]
        else:
            list = [date, date, "1"]
        data.append(list)
        df = DataFrame(data=data, columns=DatesController._columns)
        presto.insert(self, df)


if __name__ == '__main__':
    if len(sys.argv) > 1:
        DatesController().run(sys.argv[1])
    else:
        # DatesController().run(start_date='1990-12-19', end_date='2021-10-02')
        DatesController().run(-3)
        print(DatesController().is_open(-3))
        # need fix
