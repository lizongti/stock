import presto
import akshare
from retrying import retry
from tools.time import clock
from pandas import DataFrame


class StocksDataSourceUpdater(presto.DataSource):
    _catalog = 'redis'
    _schema = 'stock'
    _table = 'stocks'

    def __init__(self: object):
        super(StocksDataSourceUpdater, self).__init__(
            StocksDataSourceUpdater._catalog,
            StocksDataSourceUpdater._schema,
            StocksDataSourceUpdater._table,
        )

    def run(self: object):
        print('[%s][%s]: updating..' % (clock(), self), end='')
        self._try_update()
        print(' -> Done!')

    @retry(stop_max_attempt_number=100)
    def _try_update(self: object):
        print('.', end='')
        df = akshare.stock_info_a_code_name()
        df['key'] = df.apply(lambda x: x.code, axis=1)
        presto.insert(self, df)

    def get(self: object) -> DataFrame:
        return presto.select(self)


if __name__ == '__main__':
    StocksDataSourceUpdater().run()
