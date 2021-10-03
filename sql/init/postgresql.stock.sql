# postgresql
create schema if not exists stock;
create table if not exists stock.days(
  open double precision,
  close double precision,
  high double precision,
  low double precision,
  turnover int,
  volume double precision,
  amplitude double precision,
  change_rate double precision,
  change double precision,
  turnover_rate double precision,
  date varchar,
  code varchar,
  constraint days_primary_key_code_date primary key (code, date)
);
create index if not exists days_index_code on stock.days (code);
create index if not exists days_index_date on stock.days (date);
create table if not exists stock.indicator(
  pe double precision,
  pe_ttm double precision,
  pb double precision,
  ps double precision,
  ps_ttm double precision,
  dv double precision,
  dv_ttm double precision,
  total_mv double precision,
  date varchar,
  code varchar,
  constraint indicator_primary_key_code_date primary key (code, date)
);
create index if not exists indicator_index_code on stock.indicator (code);
create index if not exists indicator_index_date on stock.indicator (date);
create table if not exists stock.turnover_trend(
  trend int,
  date varchar,
  code varchar,
  constraint turnover_trend_primary_key_code_date primary key (code, date)
);
create index if not exists turnover_trend_index_code on stock.turnover_trend (code);
create index if not exists turnover_trend_index_date on stock.turnover_trend (date);
create table if not exists stock.turnover_rate(
  rate double precision,
  date varchar,
  code varchar,
  constraint turnover_rate_primary_key_code_date primary key (code, date)
);
create index if not exists turnover_rate_index_code on stock.turnover_rate (code);
create index if not exists turnover_rate_index_date on stock.turnover_rate (date);
create table if not exists stock.moving_average(
  ma5 double precision,
  ma10 double precision,
  ma20 double precision,
  ma30 double precision,
  ma40 double precision,
  ma60 double precision,
  ma120 double precision,
  ma200 double precision,
  ma240 double precision,
  ma250 double precision,
  date varchar,
  code varchar,
  constraint moving_average_primary_key_code_date primary key (code, date)
);
create index if not exists moving_average_index_code on stock.moving_average (code);
create index if not exists moving_average_index_date on stock.moving_average (date);
create table if not exists stock.turn(
  turn1 int,
  turn2 int,
  turn3 int,
  turn4 int,
  turn5 int,
  turn6 int,
  turn7 int,
  turn8 int,
  turn9 int,
  date varchar,
  code varchar,
  constraint turn_primary_key_code_date primary key (code, date)
);
create index if not exists turn_index_code on stock.turn (code);
create index if not exists turn_index_date on stock.turn (date);