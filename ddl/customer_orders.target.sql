CREATE TABLE db.customer_orders (
  order_aggregate_identifier binary(16) NOT NULL,
  customer_aggregate_identifier binary(16) NOT NULL,
  orderstatus varchar(2) NOT NULL,
  totalprice decimal(15,2) NOT NULL, 
  orderdate date NOT NULL,
  orderpriority varchar(10) NOT NULL,
  clerk varchar(50) NOT NULL,
  shippriority varchar(10) NOT NULL,
  comment varchar(200) NOT NULL,
  PRIMARY KEY (order_aggregate_identifier)
) ENGINE=InnoDB
