import presto.importer as importer
import akshare as ak
from retrying import retry


class MinutesDataSetUpdater(importer.DataSet):
    _catalog = 'hive'
    _schema = 'stock'
    _table = 'minutes'
    _columns = ["datetime", "opening", "closing", "higheast",
                "loweast",  "volume", "turnover", "lastest"]

    def __init__(self: object):
        super(MinutesDataSetUpdater, self).__init__(
            MinutesDataSetUpdater._catalog,
            MinutesDataSetUpdater._schema,
            MinutesDataSetUpdater._table)

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
        from datetime import date

        print('.', end='')
        df = ak.stock_zh_a_hist_min_em(symbol=code)
        df.columns = MinutesDataSetUpdater._columns
        df['time'] = df.apply(lambda x: x['datetime'].split(' ')[1], axis=1)
        df['date'] = df.apply(lambda x: x['datetime'].split(' ')[0], axis=1)
        df['code'] = df.apply(lambda x: code, axis=1)

        dt = date.today().strftime("%Y-%m-%d")
        df = df.loc[df['date'] == dt]
        importer.delete(self, {'code': code, 'date': dt})
        importer.insert(self, df)

    @retry(stop_max_attempt_number=100)
    def _get_codes(self: object) -> list[str]:
        symbols = []
        df = ak.stock_info_a_code_name()
        for row in df.iterrows():
            values = row[1].values
            key = values[0]
            symbols.append(key)
        return symbols


if __name__ == '__main__':
    MinutesDataSetUpdater().run()
