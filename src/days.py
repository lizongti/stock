
import presto
import datetime
import akshare
from retrying import retry
from tools.time import clock
from tools.math import is_int


class DaysDataSourceUpdater(presto.DataSource):
    _catalog = 'postgresql'
    _schema = 'stock'
    _table = 'days'
    _columns = ['opening', 'closing', 'higheast', 'loweast', 'volume', 'turnover',
                'amplitude', 'quote_change', 'ups_and_dows', 'turnover_rate',
                'date', 'code']
    # stock_zh_a_hist_df = ak.stock_zh_a_hist(
    #     symbol="000001", start_date="20170301", end_date='20210907', adjust="qfq")
    # print(stock_zh_a_hist_df)

    def __init__(self: object):
        super(DaysDataSourceUpdater, self).__init__(
            DaysDataSourceUpdater._catalog,
            DaysDataSourceUpdater._schema,
            DaysDataSourceUpdater._table,
        )

    def run(self: object, days: object = 0):
        dt = self._get_date(days)
        codes = self._get_codes()
        length = len(codes)
        for i in range(length):
            code = codes[i]
            print('[%s][%s][%s](%d/%d): updating..'
                  % (clock(), self, code, i+1, length), end='')
            self._update_days(code, dt)
            print(' -> Done!')

    def _delete_days(self: object, dt: str):
        presto.delete(self, {'date': dt})

    @retry(stop_max_attempt_number=100)
    def _get_codes(self: object) -> list[str]:
        symbols = []
        df = akshare.stock_info_a_code_name()
        for row in df.iterrows():
            values = row[1].values
            key = values[0]
            symbols.append(key)
        return symbols

    def _get_date(self: object, days: object) -> str:
        if isinstance(days, int) or isinstance(days, str) and is_int(days):
            now = datetime.datetime.now()
            delta = datetime.timedelta(days=int(days))
            return (now + delta).strftime('%Y-%m-%d')
        elif days is None:
            now = datetime.datetime.now()
            return now.strftime('%Y-%m-%d')
        else:
            return days
