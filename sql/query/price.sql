# higheast
select count(*) as count, time from
(select row_number() over (partition by code, date order by higheast desc) as n, time from hive.stock.minutes where date='2021-09-14')
where n = 1 group by time order by count desc;

# loweast
select count(*) as count, time from
(select row_number() over (partition by code, date order by loweast asc) as n, time from hive.stock.minutes where date='2021-09-14')
where n = 1 group by time order by count desc;