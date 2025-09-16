INSERT INTO db.orders (orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment)
SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment
FROM staging.orders s
ON DUPLICATE KEY UPDATE
orderkey = s.orderkey, custkey = s.custkey, orderstatus = s.orderstatus, totalprice = s.totalprice, orderdate = s.orderdate, orderpriority = s.orderpriority, clerk = s.clerk, shippriority = s.shippriority, comment = s.comment
