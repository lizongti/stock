if __name__ == '__main__':
    import sys
    from os.path import dirname, abspath
    sys.path.append(dirname(dirname(abspath(__file__))))

import akshare
import presto
from retrying import retry
from tools import time


class DaysController(presto.DataSource):
    _catalog = 'postgresql'
    _schema = 'stock'
    _table = 'days'
    _columns = ['date', 'opening', 'closing', 'higheast', 'loweast', 'volume', 'turnover',
                'amplitude', 'quote_change', 'ups_and_dows', 'turnover_rate']

    def __init__(self: object):
        super(DaysController, self).__init__(
            DaysController._catalog,
            DaysController._schema,
            DaysController._table,
        )

    def run(self: object, days: object = 0, **kargs):
        if len(kargs) > 0:
            start_date = kargs['start_date']
            end_date = kargs['end_date']
            codes = self._get_codes()
            length = len(codes)
            for i in range(length):
                code = codes[i]
                print('[%s][%s][%s](%d/%d): updating..'
                      % (time.clock(), self, code, i+1, length), end='')
                self._insert_days(code, start_date, end_date)
                self._insert_days(code, start_date, end_date)
                print(' -> Done!')
        else:
            date = time.date(days)
            self._delete_one_day(date)
            codes = self._get_codes()
            length = len(codes)
            for i in range(length):
                code = codes[i]
                print('[%s][%s][%s](%d/%d): updating..'
                      % (time.clock(), self, code, i+1, length), end='')
                self._insert_one_day(code, date)
                print(' -> Done!')

    @retry(stop_max_attempt_number=100)
    def _delete_one_day(self: object, date: str):
        presto.delete(self, {'date': date})

    @retry(stop_max_attempt_number=100)
    def _insert_one_day(self: object, code: str, date: str):
        print('.', end='')
        df = akshare.stock_zh_a_hist(
            symbol=code, start_date=date, end_date=date, adjust="qfq")
        df.columns = DaysController._columns
        df['code'] = df.apply(lambda x: code, axis=1)

        presto.insert(self, df.loc[df['date'] == date])

    @retry(stop_max_attempt_number=100)
    def _insert_days(self: object, code: str, start_date: str, end_date: str):
        presto.delete(self, ["code='%s'" % (code),
                             "date >= '%s' and date <= '%s'" % (start_date, end_date)])

    @retry(stop_max_attempt_number=100)
    def _insert_days(self: object, code: str, start_date: str, end_date: str):
        print('.', end='')
        df = akshare.stock_zh_a_hist(
            symbol=code, start_date=start_date, end_date=end_date, adjust="qfq")
        df.columns = DaysController._columns
        df['code'] = df.apply(lambda x: code, axis=1)

        presto.insert(self, df)

    @retry(stop_max_attempt_number=100)
    def _get_codes(self: object) -> list[str]:
        from controller import StocksController
        presto.select(StocksController())['code'].to_list()


if __name__ == '__main__':
    if len(sys.argv) > 1:
        DaysController().run(sys.argv[1])
    else:
        # DaysController().run(start_date='1990-12-19', end_date='2021-09-17')
        DaysController().run()
