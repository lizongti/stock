import datetime
if __name__ == '__main__':
    import sys
    from os.path import dirname, abspath
    sys.path.append(dirname(dirname(abspath(__file__))))

from pandas.core.frame import DataFrame
import presto
from tools import time
from retrying import retry


class TurnController(presto.DataSource):
    _catalog = 'postgresql'
    _schema = 'stock'
    _table = 'turn'

    def __init__(self: object):
        super(TurnController, self).__init__(
            TurnController._catalog,
            TurnController._schema,
            TurnController._table,
        )

    def run(self: object, days: object = 0, **kargs):
        if len(kargs) > 0:
            start_date = time.to_date(kargs['start_date'])
            end_date = time.to_date(kargs['end_date'])

            for i in range((end_date - start_date).days + 1):
                date = time.date(start_date + datetime.timedelta(days=i))
                self._update_one_day(date)
        else:
            date = time.date(days)
            self._update_one_day(date)

    def _update_one_day(self: object, date: str):
        self._delete_day(date)
        codes = self._get_codes(date)
        length = len(codes)
        for i in range(length):
            code = codes[i]

    @retry(stop_max_attempt_number=100)
    def _get_codes(self: object, date: str) -> list[str]:
        from controller import DaysController
        presto.select(DaysController(), {"date": date})['code'].to_list()


if __name__ == '__main__':
    if len(sys.argv) > 1:
        TurnController().run(sys.argv[1])
    else:
        TurnController().run(start_date='1990-12-19', end_date='2021-09-30')
