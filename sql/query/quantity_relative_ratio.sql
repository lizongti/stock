select cast(day.volume/average.volume as decimal(10, 2)) as ratio, day.code, day.date from
(select code, avg(volume) as volume from
(select code, volume, row_number() over (partition by code order by date desc) as n from postgresql.stock.days WHERE date<='2021-03-21')
where n >= 1 and n <= 5 
group by code) average,
(select code, date, volume from
(select code, date, volume, row_number() over (partition by code order by date desc) as n from postgresql.stock.days WHERE date<='2021-03-21')
where n = 1) day
where average.code = day.code