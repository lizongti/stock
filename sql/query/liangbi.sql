select avg(case when price_rate > 0 then 1 else 0 end) , avg(case when day_price_rate > 0 then 1 else 0 end) from
(select day.code, day.date, day.volume/average.volume as volume_rate, (day.closing-last_day.closing)/last_day.closing as price_rate, (day.closing-day.opening)/day.opening as day_price_rate from
(select code, avg(volume) as volume from
(select code, volume, row_number() over (partition by code order by date desc) as n from postgresql.stock.days WHERE date<='2021-09-21')
where n > 1 and n <= 6 
group by code) average,
(select code, volume, date, opening, closing from
(select code, volume, date, opening, closing, row_number() over (partition by code order by date desc) as n from postgresql.stock.days WHERE date<='2021-09-21')
where n = 1) day,
(select code, closing from
(select code, closing, row_number() over (partition by code order by date desc) as n from postgresql.stock.days WHERE date<='2021-09-21')
where n = 2) last_day
where average.code = day.code and day.code = last_day.code)
where volume_rate > 2;
