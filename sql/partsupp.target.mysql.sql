INSERT INTO db.partsupp (partkey, suppkey, availqty, supplycost, comment)
SELECT partkey, suppkey, availqty, supplycost, comment
FROM staging.partsupp s
ON DUPLICATE KEY UPDATE
partkey = s.partkey, suppkey = s.suppkey, availqty = s.availqty, supplycost = s.supplycost, comment = s.comment
