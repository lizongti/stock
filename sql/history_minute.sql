insert into orders values(1111, 'ok', 1.1, cast(date_parse('2010-01-10', '%Y-%m-%d') as date));
CREATE TABLE orders (
  orderkey bigint,
  orderstatus varchar,
  totalprice double,
  orderdate date
)
WITH (format = 'ORC', partitioned_by = ARRAY['orderdate'])