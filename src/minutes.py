import presto
import akshare
from retrying import retry
from tools.time import clock
import datetime
import sys


class MinutesDataSourceUpdater(presto.DataSource):
    _catalog = 'hive'
    _schema = 'stock'
    _table = 'minutes'
    _columns = ["datetime", "opening", "closing", "higheast",
                "loweast",  "volume", "turnover", "lastest"]

    def __init__(self: object):
        super(MinutesDataSourceUpdater, self).__init__(
            MinutesDataSourceUpdater._catalog,
            MinutesDataSourceUpdater._schema,
            MinutesDataSourceUpdater._table)

    def run(self: object, days: object = 0):
        dt = self._get_date(days)
        self._delete_minutes(dt)
        codes = self._get_codes()
        length = len(codes)
        for i in range(length):
            code = codes[i]
            print('[%s][%s][%s](%d/%d): updating..'
                  % (clock(), self, code, i+1, length), end='')
            self._insert_minutes(code, dt)
            print(' -> Done!')

    def _get_date(self: object, days: object) -> str:
        if isinstance(days, int) or isinstance(days, str) and days.isdigit():
            now = datetime.datetime.now()
            delta = datetime.timedelta(days=int(days))
            return (now + delta).strftime('%Y-%m-%d')
        elif days is None:
            now = datetime.datetime.now()
            return now.strftime('%Y-%m-%d')
        else:
            return days

    def _delete_minutes(self: object, dt: str):
        presto.delete(self, {"date": dt})

    @retry(stop_max_attempt_number=100)
    def _insert_minutes(self: object, code: str, dt: str):
        print('.', end='')
        df = akshare.stock_zh_a_hist_min_em(symbol=code)
        df.columns = MinutesDataSourceUpdater._columns
        df['time'] = df.apply(lambda x: x['datetime'].split(' ')[1], axis=1)
        df['date'] = df.apply(lambda x: x['datetime'].split(' ')[0], axis=1)
        df['code'] = df.apply(lambda x: code, axis=1)

        presto.insert(self, df.loc[df['date'] == dt])

    # @retry(stop_max_attempt_number=100)
    def _get_codes(self: object) -> list[str]:
        symbols = []
        df = akshare.stock_info_a_code_name()
        for row in df.iterrows():
            values = row[1].values
            key = values[0]
            symbols.append(key)
        return symbols


if __name__ == '__main__':
    if len(sys.argv) > 1:
        MinutesDataSourceUpdater().run(sys.argv[1])
    else:
        MinutesDataSourceUpdater().run()
