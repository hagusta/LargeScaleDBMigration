CREATE TABLE staging.customer_event (
  customer_aggregate_identifier binary(16) NOT NULL,
  event_type varchar(20) NOT NULL,
  event_timestamp integer NOT NULL,
  event_parameters JSON,
  PRIMARY KEY (customer_aggregate_identifier)
) ENGINE=InnoDB

