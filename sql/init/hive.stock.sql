# presto
create schema if not exists hive.stock;
create table if not exists hive.stock.minutes(
  open double,
  close double,
  high double,
  low double,
  turnover int,
  volume double,
  time varchar,
  code varchar,
  date varchar
) with (format = 'ORC', partitioned_by = array['date']);
create table if not exists hive.stock.minutes_partitial(
  open double,
  close double,
  high double,
  low double,
  turnover int,
  volume double,
  time varchar,
  code varchar,
  date varchar
) with (format = 'ORC', partitioned_by = array['code', 'date']);