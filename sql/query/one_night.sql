set session query_max_stage_count = 1000;
with codes as (
    with dates as (
        select date, n from (select date, row_number() over (order by date asc) as n from dates where date >= '2021-09-03' and open = '1')  where n <= 4 
    )
    select distinct rule1.code as code from 
    (
        select code from postgresql.stock.days where date in (select date from dates where n = 1) and change_rate>=3 and change_rate<=5
    ) rule1,
    (
        select code from postgresql.stock.relative_volume where date in (select date from dates where n = 1) and rvol >= 1 and rvol <= 3
    ) rule2,
    (
        select code from postgresql.stock.days where date in (select date from dates where n = 1) and turnover_rate >= 5 and turnover_rate <= 10
    ) rule3,
    (
        select code from postgresql.stock.indicator where date in (select date from dates where n = 1) and total_mv >= 5000000000 and total_mv <= 20000000000
    ) rule4,
    (
        select code from postgresql.stock.turnover_trend where date in (select date from dates where n = 1)  and trend >= 2 and trend <= 3
    ) rule5,
    (
        select code from postgresql.stock.days where date in (select date from dates where n = 1) and (high - close) / open <= 0.5
    ) rule6,
    (
        select ma.code from
        (select code, ma5 from postgresql.stock.moving_average where date in (select date from dates where n = 1)) ma,
        (select code, close from postgresql.stock.days where date in (select date from dates where n = 1)) days
        where ma.code = days.code and days.close > ma.ma5
    ) rule7
    where rule1.code = rule2.code
    and rule1.code = rule3.code
    and rule1.code = rule4.code
    and rule1.code = rule5.code
    and rule1.code = rule6.code
    and rule1.code = rule7.code
),
dates as (
    select date, n from (select date, row_number() over (order by date asc) as n from dates where date >= '2021-09-03' and open = '1')  where n <= 4 
)
select 
    status.code, 
    status.date,
    status.close,
    result1.date as day1_date,
    result1.high as day1_high,
    (result1.high - status.close) * 100/status.close as day1_rate, 
    result2.date as day2_date,
    result2.high as day2_high,
    (result2.high - status.close) * 100/status.close as day2_rate,
    result3.date as day3_date,
    result3.high as day3_high,
    (result3.high - status.close) * 100/status.close as day3_rate
from
    (select close, code, date from postgresql.stock.days where code in (select code from codes) and date in (select date from dates where n = 1) ) status,
    (select high, code, date from postgresql.stock.days where code in (select code from codes) and date in (select date from dates where n = 2) ) result1,
    (select high, code, date from postgresql.stock.days where code in (select code from codes) and date in (select date from dates where n = 3) ) result2,
    (select high, code,date from postgresql.stock.days where code in (select code from codes) and date in (select date from dates where n = 4) ) result3
where result1.code = status.code
and result2.code = status.code
and result3.code = status.code


