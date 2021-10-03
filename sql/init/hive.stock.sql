# presto
create schema if not exists hive.stock;
create table if not exists hive.stock.minutes(
  datetime varchar,
  open double,
  close double,
  high double,
  low double,
  turnover int,
  volume double,
  code varchar,
  time varchar,
  date varchar,
) with (partitioned_by = array['date'])
