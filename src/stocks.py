import presto.importer as importer
from retrying import retry
import akshare as ak


class StocksDataSetUpdater(importer.DataSet):
    _catalog = "redis"
    _schema = "stock"
    _table = "stocks"

    def __init__(self: object):
        super(StocksDataSetUpdater, self).__init__(
            StocksDataSetUpdater._catalog,
            StocksDataSetUpdater._schema,
            StocksDataSetUpdater._table)

    def update(self: object):
        print("Stocks is updating..", end='')
        self._try_update()
        print(" -> Done!")

    # @retry(stop_max_attempt_number=100)
    def _try_update(self: object):
        print(".", end='')
        df = ak.stock_info_a_code_name()
        df["key"] = df.apply(lambda x: x.code, axis=1)
        importer.load(self, df)


if __name__ == '__main__':
    stocks = StocksDataSetUpdater()
    stocks.update()
