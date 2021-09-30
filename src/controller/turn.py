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
    _columns = ['turn1', 'turn2', 'turn3', 'turn4',
                'turn5', 'turn6', 'turn7', 'turn8',
                'turn9', 'date', 'code']
    _limit = 100

    def __init__(self: object):
        super(TurnController, self).__init__(
            TurnController._catalog,
            TurnController._schema,
            TurnController._table,
        )

    def run(self: object, days: object = 0, **kargs):
        if len(kargs) > 0:
            start_date = kargs['start_date']
            end_date = kargs['end_date']
            codes = self._get_codes()
            length = len(codes)
            for i in range(length):
                code = codes[i]
                print('[%s][%s][%s](%d/%d): updating..'
                      % (time.clock(), self, code, i+1, length), end='')
                self._update_by_dates(code, start_date, end_date)
                print(' -> Done!')

        else:
            date = time.date(days)
            print('[%s][%s][%s]: updating..' %
                  (time.clock(), self, date), end='')
            self._update_by_date(date)
            print(' -> Done!')

    @retry(stop_max_attempt_number=100)
    def _update_by_dates(self: object, code: str, start_date: str, end_date: str):
        print('.', end='')
        self._delete_by_code_dates(code, start_date, end_date)
        df = self._get_days_by_code(code)
        df = df.sort_values('date')
        data = self._calc_by_dates(code, start_date, end_date, df)
        self.insert(data)

    @retry(stop_max_attempt_number=100)
    def _update_by_date(self: object,  date: str):
        print('.', end='')
        self._delete_by_date(date)
        df = self._get_days_by_date(date)
        df.sort_values('date', ascending=False)
        data = []

        codes = self._get_codes()
        length = len(codes)
        for i in range(length):
            code = codes[i]
            list = self._calc_by_date(
                code, date, df.query('code=="%s"' % (code)))
            data.append(list)

        df = DataFrame(data=data, columns=TurnController._columns)
        presto.insert(self, df)

    def _calc_by_dates(self: object, code: str, start_date: str, end_date: str, df: DataFrame):
        data = []
        turn = {}
        for index in range(0, df.shape[0]):
            for delta in range(1, 10):
                if index < delta:
                    turn[delta] = 0
                elif df.iloc[index]['closing'] > df.iloc[index-delta]['closing']:
                    if turn[delta] >= 0:
                        turn[delta] = turn[delta] + 1
                    else:
                        turn[delta] = 1
                elif df.iloc[index]['closing'] < df.iloc[index-delta]['closing']:
                    if turn[delta] <= 0:
                        turn[delta] = turn[delta] - 1
                    else:
                        turn[delta] = -1
                else:
                    if turn[delta] > 0:
                        turn[delta] = turn[delta] + 1
                    elif turn[delta] < 0:
                        turn[delta] = turn[delta] - 1
                    else:
                        turn[delta] = 0

            date = df.iloc[index]['date']
            if date >= start_date and date <= end_date:
                data.append([turn[1], turn[2], turn[3], turn[4], turn[5],
                             turn[6], turn[7], turn[8], turn[9], date, code])

        return data

    def _calc_by_date(self: object, code: str, date: str, df: DataFrame) -> list[str]:
        turn = {}
        for delta in range(1, 10):
            for index in range(df.shape[0]-1, -1, -1):
                if delta not in turn or df.shape[0] <= delta:
                    turn[delta] = 0
                elif df.iloc[index]['closing'] > df.iloc[index-delta]['closing']:
                    if turn[delta] >= 0:
                        turn[delta] = turn[delta] + 1
                    else:
                        turn[delta] = 1
                        break
                elif df.iloc[index]['closing'] < df.iloc[index-delta]['closing']:
                    if turn[delta] <= 0:
                        turn[delta] = turn[delta] - 1
                    else:
                        turn[delta] = -1
                        break
                else:
                    if turn[delta] > 0:
                        turn[delta] = turn[delta] + 1
                    elif turn[delta] < 0:
                        turn[delta] = turn[delta] - 1
                    else:
                        turn[delta] = 0

        return [turn[1], turn[2], turn[3], turn[4], turn[5],
                turn[6], turn[7], turn[8], turn[9], date, code]

    def _insert(self: object, data: list):
        df = DataFrame(data=data, columns=TurnController._columns)
        presto.insert(self, df)

    def _delete_by_date(self: object, date: str):
        presto.delete(self, {'date': date})

    def _delete_by_code_dates(self: object, code: str, start_date: str, end_date: str):
        presto.delete(self, ["code='%s'" % (code),
                             "date >= '%s' and date <= '%s'" % (start_date, end_date)])

    def _get_days_by_code(self: object, code: str) -> DataFrame:
        from controller import DaysController
        return presto.select(DaysController(), {"code": code})

    def _get_days_by_date(self: object, date: str) -> DataFrame:
        from controller import DaysController
        sql = """
            select * from (
            select *, row_number() over (partition by code order by date desc)  as n from postgresql.stock.days
            ) where n <= %d and date <= '%s'
        """ % (TurnController._limit, date)
        return presto.select(DaysController(), sql)

    @ retry(stop_max_attempt_number=100)
    def _get_codes(self: object) -> list[str]:
        from controller import StocksController
        return presto.select(StocksController())['code'].to_list()


if __name__ == '__main__':
    if len(sys.argv) > 1:
        TurnController().run(sys.argv[1])
    else:
        # TurnController().run(start_date='1990-12-19', end_date='2021-09-27')
        TurnController().run(-1)
