if __name__ == '__main__':
    import sys
    from os.path import dirname, abspath
    sys.path.append(dirname(dirname(abspath(__file__))))

import datetime
from pandas.core.frame import DataFrame
import presto
from tools import time
from retrying import retry


class QuantityTrendController(presto.DataSource):
    _catalog = 'postgresql'
    _schema = 'stock'
    _table = 'quantity_trend'
    _columes = ['trend', 'date', 'code']
    _limit = 100

    def __init__(self: object):
        super(QuantityTrendController, self).__init__(
            QuantityTrendController._catalog,
            QuantityTrendController._schema,
            QuantityTrendController._table,
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
                self._update_by_dates(code, start_date, end_date)
                print(' -> Done!')

        else:
            date = time.date(days)
            print('[%s][%s][%s]: updating..' %
                  (time.clock(), self, date), end='')
            self._update_by_date(date)
            print(' -> Done!')

    @retry(stop_max_attempt_number=100)
    def _update_by_dates(self: object, code: str, start_date: str, end_date: str):
        print('.', end='')
        self._delete_by_code_dates(code, start_date, end_date)
        df = self._get_days_by_code(code)
        df = df.sort_values('date')
        data = self._calc_by_dates(code, start_date, end_date, df)
        self._insert(data)

    @retry(stop_max_attempt_number=100)
    def _update_by_date(self: object,  date: str):
        print('.', end='')
        self._delete_by_date(date)
        df = self._get_days_by_date(date)
        df = df.sort_values('date', ascending=False)
        data = []

        codes = self._get_codes()
        length = len(codes)
        for i in range(length):
            code = codes[i]
            list = self._calc_by_date(
                code, date, df.query('code=="%s"' % (code)))
            data.append(list)

        self._insert(data)

    def _calc_by_dates(self: object, code: str, start_date: str, end_date: str, df: DataFrame):
        data = []
        trend = 0
        for index in range(1, df.shape[0]):
            if df.iloc[index]['volume'] > df.iloc[index-1]['volume']:
                if trend >= 0:
                    trend = trend + 1
                else:
                    trend = 1
            elif df.iloc[index]['volume'] < df.iloc[index-1]['volume']:
                if trend <= 0:
                    trend = trend - 1
                else:
                    trend = -1
            else:
                if trend > 0:
                    trend = trend + 1
                elif trend < 0:
                    trend = trend - 1
                else:
                    trend = 0

            date = df.iloc[index]['date']
            if date >= start_date and date <= end_date:
                data.append([trend, date, code])

        return data

    def _calc_by_date(self: object, code: str, date: str, df: DataFrame) -> list[str]:
        trend = 0
        for index in range(df.shape[0]-2, -1, -1):
            if df.iloc[index]['closing'] > df.iloc[index-1]['closing']:
                if trend >= 0:
                    trend = trend + 1
                else:
                    trend = 1
                    break
            elif df.iloc[index]['closing'] < df.iloc[index-1]['closing']:
                if trend <= 0:
                    trend = trend - 1
                else:
                    trend = -1
                    break
            else:
                if trend > 0:
                    trend = trend + 1
                elif trend < 0:
                    trend = trend - 1
                else:
                    trend = 0

        return [trend, date, code]

    def _insert(self: object, data: list):
        df = DataFrame(data=data, columns=QuantityTrendController._columes)
        presto.insert(self, df)

    def _delete_by_date(self: object, date: str):
        presto.delete(self, {'date': date})

    def _delete_by_code_dates(self: object, code: str, start_date: str, end_date: str):
        presto.delete(self, ["code='%s'" % (code),
                             "date >= '%s' and date <= '%s'" % (start_date, end_date)])

    def _get_days_by_code(self: object, code: str) -> DataFrame:
        from controller import DaysController
        return presto.select(DaysController(), {"code": code})

    def _get_days_by_date(self: object, date: str) -> DataFrame:
        from controller import DaysController
        sql = """
            select * from (
            select *, row_number() over (partition by code order by date desc)  as n from postgresql.stock.days
            ) where n <= %d and date <= '%s'
        """ % (QuantityTrendController._limit, date)
        return presto.select(DaysController(), sql)

    @retry(stop_max_attempt_number=100)
    def _get_codes(self: object) -> list[str]:
        from controller import StocksController
        return presto.select(StocksController())['code'].to_list()


if __name__ == '__main__':
    if len(sys.argv) > 1:
        QuantityTrendController().run(sys.argv[1])
    else:
        # QuantityTrendController().run(start_date='1990-12-19', end_date='2021-09-30')
        QuantityTrendController().run(-1)
