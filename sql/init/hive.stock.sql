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
  time varchar,
  date varchar,
  code varchar
) with (partitioned_by = array['date', 'code'])