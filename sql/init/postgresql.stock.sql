# postgresql
create schema if not exists stock;
create table if not exists stock.days(
  opening double precision,
  closing double precision,
  higheast double precision,
  loweast double precision,
  volume int,
  turnover double precision,
  amplitude double precision,
  quote_change double precision,
  ups_and_dows double precision,
  turnover_rate double precision,
  date varchar,
  code varchar,
  constraint days_primary_key_code_date primary key (code, date)
);
create index if not exists days_index_code on stock.days (code);
create index if not exists days_index_date on stock.days (date);
create table if not exists stock.quantity_ratio(
  ratio double precision,
  date varchar,
  code varchar,
  constraint quantity_ratio_primary_key_code_date primary key (code, date)
);
create index if not exists quantity_ratio_index_code on stock.quantity_ratio (code);
create index if not exists quantity_ratio_index_date on stock.quantity_ratio (date);
create table if not exists stock.quantity_trend(
  trend int,
  date varchar,
  code varchar,
  constraint quantity_trend_primary_key_code_date primary key (code, date)
);
create index if not exists quantity_trend_index_code on stock.quantity_trend (code);
create index if not exists quantity_trend_index_date on stock.quantity_trend (date);
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
