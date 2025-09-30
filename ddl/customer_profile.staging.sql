CREATE TABLE staging.customer_profile (
  custkey int NOT NULL,
  customer_aggregate_identifier binary(16) NOT NULL,
  name varchar(100) NOT NULL,
  address varchar(100) NOT NULL,
  country varchar(50) NOT NULL,
  phone varchar(20) NOT NULL,
  account_balance decimal(15,2) NOT NULL,
  total_purchase_order decimal(15,2) NOT NULL,
	total_net_lineitem decimal(15,2) NOT NULL,
	total_gross_lineitem decimal(15,2) NOT NULL,
	total_discount_lineitem decimal(15,2) NOT NULL,
	total_tax_lineitem decimal(15,2) NOT NULL,
  market_segment varchar(50) NOT NULL,
  comment varchar(200) NOT NULL,
  PRIMARY KEY(aggregate_identifier)
 ) ENGINE=InnoDB
