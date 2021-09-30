if __name__ == '__main__':
    import sys
    from os.path import dirname, abspath
    sys.path.append(dirname(dirname(abspath(__file__))))

import sys
import presto
import akshare
from retrying import retry
from tools import time


class MinutesController(presto.DataSource):
    _catalog = 'hive'
    _schema = 'stock'
    _table = 'minutes'
    _rename_columns = ['datetime', 'opening', 'closing', 'higheast',
                        'loweast',  'volume', 'turnover', 'lastest']

    def __init__(self: object):
        super(MinutesController, self).__init__(
            MinutesController._catalog,
            MinutesController._schema,
            MinutesController._table,
        )

    def run(self: object, days: object = 0):
        date = time.date(days)
        self._delete_minutes(date)
        codes = self._get_codes()
        length = len(codes)
        for i in range(length):
            code = codes[i]
            print('[%s][%s][%s](%d/%d): updating..'
                  % (time.clock(), self, code, i+1, length), end='')
            self._insert_minutes(code, date)
            print(' -> Done!')

    def _delete_minutes(self: object, date: str):
        presto.delete(self, {'date': date})

    @retry(stop_max_attempt_number=100)
    def _insert_minutes(self: object, code: str, date: str):
        print('.', end='')
        df = akshare.stock_zh_a_hist_min_em(symbol=code)
        df.columns = MinutesController._rename_columns
        df['time'] = df.apply(lambda x: x['datetime'].split(' ')[1], axis=1)
        df['date'] = df.apply(lambda x: x['datetime'].split(' ')[0], axis=1)
        df['code'] = df.apply(lambda x: code, axis=1)

        presto.insert(self, df.loc[df['date'] == date])

    @retry(stop_max_attempt_number=100)
    def _get_codes(self: object) -> list[str]:
        from controller import StocksController
        return presto.select(StocksController())['code'].to_list()


if __name__ == '__main__':
    if len(sys.argv) > 1:
        MinutesController().run(sys.argv[1])
    else:
        MinutesController().run()
