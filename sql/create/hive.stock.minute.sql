create table hive.default.minute(
  time varchar,
  opening double,
  closing double,
  higheast double,
  loweast double,
  volume double,
  turnover double,
  latest double,
  code varchar,
  date varchar
) with (partitioned_by = array['code', 'date'])