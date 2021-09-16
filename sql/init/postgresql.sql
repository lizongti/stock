# postgresql
create schema if not exists stock;
create table if not exists stock.days(
  opening money,
  closing money,
  higheast money,
  loweast money,
  volume int,
  turnover money,
  amplitude money,
  quote_change money,
  ups_and_dows money,
  turnover_rate money,
  date varchar,
  code varchar,
  constraint primary_key_ primary key (code, date)
);