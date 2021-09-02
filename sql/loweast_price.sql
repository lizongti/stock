select count(1) as count, m
from
    (select day, m, loweast, row_number() over (partition by day order by loweast asc) num
    from (select substr(time,1,10) as day, substr(time,12,5) as m, *
        from minute))
where num = 1
group by m;