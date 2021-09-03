select count(*) as count, time
from
(select row_number() over (partition by symbol, date order by loweast asc) as num, time from minute)
where num = 1
group by time;