INSERT INTO db.lineitem (orderkey, partkey, suppkey, linenumber, quantity, extendedprice, discount, tax, returnflag, linestatus, shipdate, commitdate, receiptdate, shipinstruct, shipmode, comment)
SELECT orderkey, partkey, suppkey, linenumber, quantity, extendedprice, discount, tax, returnflag, linestatus, shipdate, commitdate, receiptdate, shipinstruct, shipmode, comment
FROM staging.lineitem s
ON DUPLICATE KEY UPDATE
orderkey = s.orderkey, partkey = s.partkey, suppkey = s.suppkey, linenumber = s.linenumber, quantity = s.quantity, extendedprice = s.extendedprice, discount = s.discount, tax = s.tax, returnflag = s.returnflag, linestatus = s.linestatus, shipdate = s.shipdate, commitdate = s.commitdate, receiptdate = s.receiptdate, shipinstruct = s.shipinstruct, shipmode = s.shipmode, comment = s.comment
