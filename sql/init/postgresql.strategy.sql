
create schema if not exists strategy;
create table if not exists strategy.one_night_strategy(
  quote_change int,
  quantity_ratio int,
  turnover_rate int,
  market_value int,
  minutes_higheast int,
  minutes_average int,
  moving_average int,
  quantity_trend int,
  date varchar,
  code varchar,
  constraint indicator_primary_key_code_date primary key (code, date)
)
create index if not exists one_night_strategy_index_code on stock.one_night_strategy (code);
create index if not exists one_night_strategy_index_date on stock.one_night_strategy (date);