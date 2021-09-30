# presto
create schema if not exists hive.stock;
create table if not exists hive.stock.minutes(
  datetime varchar,
  opening double,
  closing double,
  higheast double,
  loweast double,
  volume int,
  turnover double,
  lastest double,
  time varchar,
  date varchar,
  code varchar
) with (partitioned_by = array['date', 'code'])
create table if not exists hive.stock.minutes_indicator(
  price double,
  higheast double,
  loweast double,
  average double,
  state int,
  ratio double,
  ma5_state int,
  ma10_state int,
  ma20_state int,
  ma30_state int,
  ma40_state int,
  ma60_state int,
  ma120_state int,
  ma200_state int,
  ma240_state int,
  ma250_state int,
  time varchar,
  date varchar,
  code varchar
) with (partitioned_by = array['date', 'code'])