INSERT INTO db.customer_orders (order_aggregate_identifier, customer_aggregate_identifier, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment)
SELECT order_aggregate_identifier, customer_aggregate_identifier, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment
FROM staging.customer_orders s
  ON DUPLICATE KEY UPDATE 
order_aggregate_identifier = s.order_aggregate_identifier, customer_aggregate_identifier = s.customer_aggregate_identifier, orderstatus = s.orderstatus, totalprice = s.totalprice, orderdate = s.orderdate, orderpriority = s.orderpriority, clerk = s.clerk, shippriority = s.shippriority, comment = s.comment
