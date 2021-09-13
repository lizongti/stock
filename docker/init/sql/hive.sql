create schema hive.stock;
create table hive.stock.minutes(
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