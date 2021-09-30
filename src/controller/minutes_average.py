if __name__ == '__main__':
    import sys
    from os.path import dirname, abspath
    sys.path.append(dirname(dirname(abspath(__file__))))

import datetime
from pandas.core.frame import DataFrame
import presto
from tools import time
from retrying import retry


class MinutesAverageController(presto.DataSource):
    _catalog = 'hive'
    _schema = 'stock'
    _table = 'minutes'
    _columns = ['price', 'higheast', 'loweast', 'average',
                'state', 'time', 'date', 'code']

    def __init__(self: object):
        super(MinutesAverageController, self).__init__(
            MinutesAverageController._catalog,
            MinutesAverageController._schema,
            MinutesAverageController._table,
        )

    def run(self: object, days: object = 0, **kargs):
        if len(kargs) > 0:
            start_date_object = time.to_date(kargs['start_date'])
            end_date_object = time.to_date(kargs['end_date'])

            codes = self._get_codes()
            for i in range((end_date_object - start_date_object).days + 1):
                date = time.date(start_date_object +
                                 datetime.timedelta(days=i))
                length = len(codes)
                for i in range(length):
                    code = codes[i]
                    print('[%s][%s][%s][%s](%d/%d): updating..' %
                          (time.clock(), self, date, code, i+1, length), end='')
                    self._update_by_code_date(code, date)
                    print(' -> Done!')
        else:
            date = time.date(days)
            codes = self._get_codes()
            length = len(codes)
            for i in range(length):
                code = codes[i]
                print('[%s][%s][%s][%s](%d/%d): updating..' %
                      (time.clock(), self, date, code, i+1, length), end='')
                self._update_by_code_date(code, date)
                print(' -> Done!')

    def _update_by_code_date(self: object, code: str, date: str):
        df = self._get_minutes_by_code_date(code, date)
        df = df.sort_values('datetime')
        data = self._calc_by_code_date(code, date, df)
        self.insert(data)

    def _calc_by_code_date(self: object, code: str, date: str, df: DataFrame) -> list[list]:
        data = []
        sum = 0
        higheast = None
        loweast = None
        for index in range(0, df.shape[0]):
            price = df.iloc[index]['closing']
            if higheast is None:
                higheast = price
            elif higheast < price:
                higheast = price
            if loweast is None:
                loweast = price
            elif loweast > price:
                loweast = price
            sum += price
            avg = sum/(index+1)
            if price > avg:
                state = 1
            elif price < avg:
                state = -1
            else:
                state = 0
            data.append(
                [price, higheast, loweast, avg, state, df.iloc[index]['time'], date, code])
        return data

    def _insert(self: object, data: list):
        df = DataFrame(data=data, columns=MinutesAverageController._columns)
        presto.insert(self, df)

    def _get_minutes_by_code_date(self: object, code: str, date: str) -> DataFrame:
        from controller import MinutesController
        return presto.select(MinutesController(), {'code': code, 'date': date})

    @retry(stop_max_attempt_number=100)
    def _get_codes(self: object) -> list[str]:
        from controller import StocksController
        return presto.select(StocksController())['code'].to_list()


if __name__ == '__main__':
    if len(sys.argv) > 1:
        MinutesAverageController().run(sys.argv[1])
    else:
        # QuantityTrendController().run(start_date='1990-12-19', end_date='2021-09-30')
        MinutesAverageController().run(-1)
