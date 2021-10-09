
create schema if not exists strategy;
create table if not exists strategy.one_night(
  close double precision,
  day1_date varchar,
  day1_high double precision,
  day1_rate double precision, 
  day2_date varchar,
  day2_high double precision,
  day2_rate double precision,
  day3_date varchar,
  day3_high double precision,
  day3_rate double precision,
  change_rate_min double precision,
  change_rate_max double precision,
  rvol_min double precision,
  rvol_max double precision,
  turnover_rate_min double precision,
  turnover_rate_max double precision,
  total_mv_min double precision,
  total_mv_max double precision,
  trend_min double precision,
  trend_max double precision,
  fall_min double precision,
  fall_max double precision,
  date varchar,
  code varchar
)