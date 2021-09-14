from pandas.core.frame import DataFrame
import presto
import akshare
from retrying import retry
from datetime import date


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

    def run(self: object):
        codes = self._get_codes()
        length = len(codes)
        for i in range(length):
            code = codes[i]
            print('[%s][%s](%d/%d): updating..'
                  % (self, code, i+1, length), end='')
            self._update_minutes(code)
            print(' -> Done!')

    @retry(stop_max_attempt_number=100)
    def _update_minutes(self: object, code: str):
        print('.', end='')
        df = akshare.stock_zh_a_hist_min_em(symbol=code)
        df.columns = MinutesDataSourceUpdater._columns
        df['time'] = df.apply(lambda x: x['datetime'].split(' ')[1], axis=1)
        df['date'] = df.apply(lambda x: x['datetime'].split(' ')[0], axis=1)
        df['code'] = df.apply(lambda x: code, axis=1)

        for dt in df['date'].unique():
            presto.delete(self, {'code': code, 'date': dt})
            presto.insert(self, df.loc[df['date'] == dt])

    @retry(stop_max_attempt_number=100)
    def _get_codes(self: object) -> list[str]:
        symbols = []
        df = akshare.stock_info_a_code_name()
        for row in df.iterrows():
            values = row[1].values
            key = values[0]
            symbols.append(key)
        return symbols


if __name__ == '__main__':
    MinutesDataSourceUpdater().run(False)
