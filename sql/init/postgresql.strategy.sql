
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
  date varchar,
  code varchar,
  constraint indicator_primary_key_code_date primary key (code, date)
)