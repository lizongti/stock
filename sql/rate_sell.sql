select avg(b.opening/a.opening) as rate, b.time from
(select symbol, date, time, opening from minute) b inner join
(select symbol, date, opening from (select row_number() over (partition by symbol, date order by time asc) as num, symbol, date, opening from minute) where num = 1) a
ON a.symbol = b.symbol and a.date = b.date group by b.time order by rate desc;