select code from 
(
    select code, turnover/total_mv as turnover_ratio from
    (
        select code, turnover from postgresql.stock.days where date = '2021-09-28' and quote_change>=3 and quote_change<=5
    ) days,
    (
        select code from postgresql.stock.quantity_ratio where date='2021-09-28' and ratio > 1
    ) quantity_ratio,
    (
        select code, total_mv from postgresql.stock.indicator where date='2021-09-28' and total_mv >= 5000000000 and total_mv <= 20000000000
    ) indicator,
    (
        select a.code, a.max_price from
        (select code, closing from minutes where date = '2021-09-28' and time = '14:30') a,
        (select code, max(closing) as max_price from hive.stock.minutes where date = '2021-09-28' and time <= '14:30' group by code) b
        where a.closing = b.max_price
    ) minutes,
    (
        select code, ma5 from postgresql.stock.moving_average where date = '2021-09-28'
    ) moving_average
    where days.code = quantity_ratio.code 
    and days.code = indicator.code 
    and days.code = minutes.code
    and days.code = moving_average.code
) 
where turnover_ratio >= 5 and turnover_ratio <= 10
and moving_average.ma5 <= minutes.max_price

# 全天 高于分时均线 大盘分时均线
# 持续放量