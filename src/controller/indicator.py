if __name__ == '__main__':
    import sys
    from os.path import dirname, abspath
    sys.path.append(dirname(dirname(abspath(__file__))))

import akshare
import presto
from retrying import retry
from tools import time


class IndicatorController(presto.DataSource):
    _catalog = 'postgresql'
    _schema = 'stock'
    _table = 'indicator'
    _columns = ['date', 'pe', 'pe_ttm', 'pb', 'ps', 'ps_ttm',
                'dv', 'dv_ttm', 'total_mv']

    def __init__(self: object):
        super(IndicatorController, self).__init__(
            IndicatorController._catalog,
            IndicatorController._schema,
            IndicatorController._table,
        )

    def run(self: object, days: object = 0, **kargs):
        if len(kargs) > 0:
            start_date = kargs['start_date']
            end_date = kargs['end_date']
            self._delete_by_dates(start_date, end_date)
            codes = self._get_codes()
            length = len(codes)
            for i in range(length):
                code = codes[i]
                print('[%s][%s][%s](%d/%d): updating..'
                      % (time.clock(), self, code, i+1, length), end='')
                self._insert_by_dates(code, start_date, end_date)
                print(' -> Done!')
        else:
            date = time.date(days)
            self._delete_by_date(date)
            codes = self._get_codes()
            length = len(codes)
            for i in range(length):
                code = codes[i]
                print('[%s][%s][%s](%d/%d): updating..'
                      % (time.clock(), self, code, i+1, length), end='')
                self._insert_by_date(code, date)
                print(' -> Done!')

    @retry(stop_max_attempt_number=100)
    def _delete_by_date(self: object, date: str):
        presto.delete(self, {'date': date})

    @retry(stop_max_attempt_number=100)
    def _delete_by_dates(self: object,  start_date: str, end_date: str):
        presto.delete(self, ["date >= '%s' and date <= '%s'" %
                      (start_date, end_date)])

    @retry(stop_max_attempt_number=100)
    def _insert_by_date(self: object, code: str, date: str):
        print('.', end='')
        df = akshare.stock_a_lg_indicator(code)
        df.columns = IndicatorController._columns
        df['date'] = df['date'].map(lambda x: time.date(x))
        df['code'] = df.apply(lambda x: code, axis=1)
        df = df.query('date=="%s"' % (date))
        presto.insert(self, df.loc[df['date'] == date])

    @retry(stop_max_attempt_number=100)
    def _insert_by_dates(self: object, code: str, start_date: str, end_date: str):
        print('.', end='')
        df = akshare.stock_a_lg_indicator(code)
        df.columns = IndicatorController._columns
        df['date'] = df['date'].map(lambda x: time.date(x))
        df['code'] = df.apply(lambda x: code, axis=1)
        df = df.query('date>="%s" and date<="%s"' % (start_date, end_date))
        presto.insert(self, df)

    @retry(stop_max_attempt_number=100)
    def _get_codes(self: object) -> list[str]:
        from controller import StocksController
        return presto.select(StocksController())['code'].to_list()


if __name__ == '__main__':
    if len(sys.argv) > 1:
        IndicatorController().run(sys.argv[1])
    else:
        IndicatorController().run(start_date='1990-12-19', end_date='2021-09-30')
        # IndicatorController().run()
