select code from 
(
    select code from postgresql.stock.days where date = '2021-09-28' and quote_change>=3 and quote_change<=5
) rule1,
(
    select code from postgresql.stock.quantity_ratio where date='2021-09-28' and ratio > 1
) rule2,
(
    select code from postgresql.stock.turnover_ratio where date='2021-09-28' and ratio >=5 and ratio <= 10
) rule3,
(
    select code from postgresql.stock.indicator where date='2021-09-28' and total_mv >= 5000000000 and total_mv <= 20000000000
) rule4,
(
    select code from postgresql.stock.quantity_trend where date = '2021-09-28' and trend >= 3
) rule5,
(
    select code from postgresql.stock.minutes_indicator where date = '2021-09-28' and time = '14:30' # 有支撑
) rule6,
(
    select code from hive.stock.minutes_indicator where date = '2021-09-28' and time <= '14:30' group by code having sum(state) >= 200 and sum(ratio_state) >= 200
) rule7,
(
    select code from hive.stock.minutes_indicator where date = '2021-09-28' and time = '14:30' where higheast <= price
) rule8
where rule1.code = rule2.code
and rule1.code = rule2.code
and rule1.code = rule3.code
and rule1.code = rule4.code
and rule1.code = rule5.code
and rule1.code = rule6.code
and rule1.code = rule7.code
and rule1.code = rule8.code