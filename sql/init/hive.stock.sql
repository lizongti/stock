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
