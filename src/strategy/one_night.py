if __name__ == '__main__':
    import sys
    from os.path import dirname, abspath
    sys.path.append(dirname(dirname(abspath(__file__))))

import presto
from pandas.core.frame import DataFrame
from retrying import retry
from tools import time
import datetime


class OneNightStrategy(presto.DataSource):
    _catalog = 'postgresql'
    _schema = 'strategy'
    _table = 'one_night'

    def __init__(self: object):
        super(OneNightStrategy, self).__init__(
            OneNightStrategy._catalog,
            OneNightStrategy._schema,
            OneNightStrategy._table,
        )

    def run(self: object, **kargs):
        start_date_object = time.to_date(kargs['start_date'])
        end_date_object = time.to_date(kargs['end_date'])

        for i in range((end_date_object - start_date_object).days + 1):
            date = time.date(start_date_object +
                             datetime.timedelta(days=i))
            print('[%s][%s][%s]: updating..' %
                  (time.clock(), self, date), end='')
            self._execute(date)
            print(' -> Done!')

    @retry(stop_max_attempt_number=100)
    def _execute(self: object, date: str) -> DataFrame:
        print('.', end='')
        args = [3, 5, 1, 3, 5, 10, 5, 20, 2, 3, 0, 0.5]
        sql = """
            insert into postgresql.strategy.one_night 
            with codes as (
                with dates as (
                    select date, n from (select date, row_number() over (order by date asc) as n from redis.stock.dates where date >= '%s' and open = '1')  where n <= 4 
                )
                select distinct rule1.code as code from 
                (
                    select code from postgresql.stock.days where date in (select date from dates where n = 1) and change_rate >= %s and change_rate <= %s
                ) rule1,
                (
                    select code from postgresql.stock.relative_volume where date in (select date from dates where n = 1) and rvol >= %s and rvol <= %s
                ) rule2,
                (
                    select code from postgresql.stock.days where date in (select date from dates where n = 1) and turnover_rate >= %s and turnover_rate <= %s
                ) rule3,
                (
                    select code from postgresql.stock.indicator where date in (select date from dates where n = 1) and total_mv/1000000000 >= %s  and total_mv/1000000000 <= %s
                ) rule4,
                (
                    select code from postgresql.stock.turnover_trend where date in (select date from dates where n = 1)  and trend >= %s and trend <= %s
                ) rule5,
                (
                    select code from postgresql.stock.days where date in (select date from dates where n = 1) and (high - close) / open >= %s and (high - close) / open <= %s
                ) rule6
                where rule1.code = rule2.code
                and rule1.code = rule3.code
                and rule1.code = rule4.code
                and rule1.code = rule5.code
                and rule1.code = rule6.code
            ),
            dates as (
                select date, n from (select date, row_number() over (order by date asc) as n from redis.stock.dates where date >= '%s' and open = '1')  where n <= 4 
            )
            select 
                status.close,
                result1.date as day1_date,
                result1.high as day1_high,
                (result1.high - status.close) * 100/status.close as day1_rate, 
                result2.date as day2_date,
                result2.high as day2_high,
                (result2.high - status.close) * 100/status.close as day2_rate,
                result3.date as day3_date,
                result3.high as day3_high,
                (result3.high - status.close) * 100/status.close as day3_rate,
                %s as change_rate_min,
                %s as change_rate_max,
                %s as rvol_min,
                %s as rvol_max,
                %s as turnover_rate_min,
                %s as turnover_rate_max,
                %s as total_mv_min,
                %s as total_mv_max,
                %s as trend_min,
                %s as trend_max,
                %s as fall_min,
                %s as fall_max,
                status.date,
                status.code
            from
                (select close, code, date from postgresql.stock.days where code in (select code from codes) and date in (select date from dates where n = 1) ) status,
                (select high, code, date from postgresql.stock.days where code in (select code from codes) and date in (select date from dates where n = 2) ) result1,
                (select high, code, date from postgresql.stock.days where code in (select code from codes) and date in (select date from dates where n = 3) ) result2,
                (select high, code,date from postgresql.stock.days where code in (select code from codes) and date in (select date from dates where n = 4) ) result3
            where result1.code = status.code
            and result2.code = status.code
            and result3.code = status.code
        """ % (date, args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7], args[8], args[9], args[10], args[11],
               date, args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7], args[8], args[9], args[10], args[11])
        presto.delete(self, {'date': date})
        presto.insert(self, sql)


if __name__ == '__main__':
    OneNightStrategy().run(start_date='1990-12-19', end_date='2021-09-30')
