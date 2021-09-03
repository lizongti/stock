select avg(b.closing/a.closing) as rate, b.time from
(select symbol, date, time, closing from minute) b inner join
(select symbol, date, closing from (select row_number() over (partition by symbol, date order by time desc) as num, symbol, date, closing from minute) where num = 1) a
ON a.symbol = b.symbol and a.date = b.date group by b.time order by rate desc;