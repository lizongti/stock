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
    _columes = ['turn1', 'turn2', 'turn3', 'turn4',
                'turn5', 'turn6', 'turn7', 'turn8',
                'turn9', 'date', 'code']

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
                self._update_by_date(date)
        else:
            date = time.date(days)
            self._update_by_date(date)

    def _update_by_date(self: object, date: str):
        codes = self._get_codes(date)
        length = len(codes)
        for i in range(length):
            code = codes[i]
            print('[%s][%s][%s][%s](%d/%d): updating..' %
                  (time.clock(), self, date, code, i+1, length), end='')
            self._update_by_date_code(date, code)
            print(' -> Done!')

    # @retry(stop_max_attempt_number=100)
    def _update_by_date_code(self: object, date: str, code: str):
        print('.', end='')
        self._delete_by_date_code(date, code)
        df = self._get_code_days(code)
        turn = self._calc_code_turn(df)
        self._insert_by_date_code(turn, date, code)

    def _insert_by_date_code(self: object, turn: dict[int, int], date: str, code: str):
        data = DataFrame(data=[[turn[1], turn[2], turn[3], turn[4], turn[5],
                                turn[6], turn[7], turn[8], turn[9], date, code]],
                         columns=TurnController._columes, index=[0])
        presto.insert(self, data)

    def _calc_code_turn(self: object, df: DataFrame) -> dict[int, int]:
        df.sort_values('date')
        turn = {}
        for index in range(0, df.shape[0]):
            for delta in range(1, 10):
                if index < delta:
                    turn[delta] = 0
                elif df.iloc[index]['closing'] > df.iloc[index-delta]['closing']:
                    if turn[delta] >= 0:
                        turn[delta] = turn[delta] + 1
                    else:
                        turn[delta] = -1
                else:
                    if turn[delta] >= 0:
                        turn[delta] = 1
                    else:
                        turn[delta] = turn[delta] - 1
        return turn

    def _delete_by_date_code(self: object, date: str, code: str):
        presto.delete(self, {'date': date, 'code': code})

    def _get_code_days(self: object, code: str) -> DataFrame:
        from controller import DaysController
        return presto.select(DaysController(), {"code": code})

    # @retry(stop_max_attempt_number=100)
    def _get_codes(self: object, date: str) -> list[str]:
        from controller import DaysController
        return presto.select(DaysController(), {"date": date})['code'].to_list()


if __name__ == '__main__':
    if len(sys.argv) > 1:
        TurnController().run(sys.argv[1])
    else:
        # TurnController().run(start_date='1990-12-19', end_date='2021-09-30')
        TurnController().run()
