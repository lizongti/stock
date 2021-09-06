select avg(b.closing/a.closing)-1 as rate, b.time from
(select symbol, date, time, closing from minute) b inner join
(
select * from
(select a1.symbol, a1.date, case when a2.closing > a1.closing then 1 else -1 end as state, a1.closing from
(select symbol, date, closing from (select row_number() over (partition by symbol, date order by time asc) as num, symbol, date, closing from minute) where num = 1) a1,
(select symbol, date, closing from (select row_number() over (partition by symbol, date order by time desc) as num, symbol, date, closing from minute) where num = 1) a2
where a1.symbol = a2.symbol and a1.date = a2.date)
where state > 0
) a
ON a.symbol = b.symbol and a.date = b.date group by b.time order by rate asc;
