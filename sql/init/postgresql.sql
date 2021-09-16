# postgresql
create schema if not exists stock;
create table if not exists stock.days(
  opening numeric,
  closing numeric,
  higheast numeric,
  loweast numeric,
  volume int,
  turnover numeric,
  amplitude numeric,
  quote_change numeric,
  ups_and_dows numeric,
  turnover_rate numeric,
  date varchar,
  code varchar,
  constraint primary_key_ primary key (code, date)
);