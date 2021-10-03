if __name__ == '__main__':
    import sys
    from os.path import dirname, abspath
    sys.path.append(dirname(dirname(abspath(__file__))))


import akshare
import presto
from retrying import retry
from tools import time


class StocksController(presto.DataSource):
    _catalog = 'redis'
    _schema = 'stock'
    _table = 'stocks'

    def __init__(self: object):
        super(StocksController, self).__init__(
            StocksController._catalog,
            StocksController._schema,
            StocksController._table,
        )

    def run(self: object):
        print('[%s][%s]: updating..' % (time.clock(), self), end='')
        self._update()
        print(' -> Done!')

    @retry(stop_max_attempt_number=100)
    def _update(self: object):
        print('.', end='')
        df = akshare.stock_info_a_code_name()
        df['key'] = df.apply(lambda x: x.code, axis=1)
        presto.insert(self, df)


if __name__ == '__main__':
    StocksController().run()
