# postgresql
create schema if not exists stock;
drop table if exists stock.days;
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
  constraint primary_key_code_date primary key (code, date)
);
create table if not exists stock.quantity_ratio(
  ratio double precision,
  date varchar,
  code varchar,
  constraint quantity_ratio_primary_key_code_date primary key (code, date)
)