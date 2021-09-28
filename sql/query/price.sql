# higheast
select count(*) as count, time from
(select row_number() over (partition by code, date order by higheast desc) as n, time from hive.stock.minutes where date>'2015-01-01')
where n = 1 group by time order by count desc;

# loweast
select count(*) as count, time from
(select row_number() over (partition by code, date order by loweast asc) as n, time from hive.stock.minutes where date>'2015-01-01')
where n = 1 group by time order by count desc;

# up higheast
select count(*) as count, time from
(select code, date, time from (select code, date, row_number() over (partition by code, date order by higheast desc) as n, time from hive.stock.minutes where date>'2015-01-01') where n = 1) minutes,
(select code, date from postgresql.stock.days where closing > opening and date>'2015-01-01') days
where minutes.date = days.date and minutes.code = days.code
group by minutes.time order by count desc;

# up loweast
select count(*) as count, time from
(select code, date, time from (select code, date, row_number() over (partition by code, date order by higheast asc) as n, time from hive.stock.minutes where date>'2015-01-01') where n = 1) minutes,
(select code, date from postgresql.stock.days where closing > opening and date>'2015-01-01') days
where minutes.date = days.date and minutes.code = days.code
group by minutes.time order by count desc;

# down higheast
select count(*) as count, time from
(select code, date, time from (select code, date, row_number() over (partition by code, date order by higheast desc) as n, time from hive.stock.minutes where date>'2015-01-01') where n = 1) minutes,
(select code, date from postgresql.stock.days where closing < opening and date>'2015-01-01') days
where minutes.date = days.date and minutes.code = days.code
group by minutes.time order by count desc;

# down loweast
select count(*) as count, time from
(select code, date, time from (select code, date, row_number() over (partition by code, date order by higheast asc) as n, time from hive.stock.minutes where date>'2015-01-01') where n = 1) minutes,
(select code, date from postgresql.stock.days where closing < opening and date>'2015-01-01') days
where minutes.date = days.date and minutes.code = days.code
group by minutes.time order by count desc;