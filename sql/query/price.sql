# high
select count(*) as count, time from
(select row_number() over (partition by code, date order by high desc) as n, time from hive.stock.minutes)
where n = 1 group by time order by count desc;

# low
select count(*) as count, time from
(select row_number() over (partition by code, date order by low asc) as n, time from hive.stock.minutes)
where n = 1 group by time order by count desc;

# up high
select count(*) as count, time from
(select code, date, time from (select code, date, row_number() over (partition by code, date order by high desc) as n, time from hive.stock.minutes) where n = 1) minutes,
(select code, date from postgresql.stock.days where close > open) days
where minutes.date = days.date and minutes.code = days.code
group by minutes.time order by count desc;

# up low
select count(*) as count, time from
(select code, date, time from (select code, date, row_number() over (partition by code, date order by low asc) as n, time from hive.stock.minutes) where n = 1) minutes,
(select code, date from postgresql.stock.days where close > open) days
where minutes.date = days.date and minutes.code = days.code
group by minutes.time order by count desc;

# down high
select count(*) as count, time from
(select code, date, time from (select code, date, row_number() over (partition by code, date order by high desc) as n, time from hive.stock.minutes) where n = 1) minutes,
(select code, date from postgresql.stock.days where close < open) days
where minutes.date = days.date and minutes.code = days.code
group by minutes.time order by count desc;

# down low
select count(*) as count, time from
(select code, date, time from (select code, date, row_number() over (partition by code, date order by low asc) as n, time from hive.stock.minutes) where n = 1) minutes,
(select code, date from postgresql.stock.days where close < open) days
where minutes.date = days.date and minutes.code = days.code
group by minutes.time order by count desc;