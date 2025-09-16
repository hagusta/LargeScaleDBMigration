INSERT INTO db.customer (custkey, name, address, nationkey, phone, acctbal, mktsegment, comment)
SELECT custkey, name, address, nationkey, phone, acctbal, mktsegment, comment
FROM staging.customer s
ON CONFLICT custkey DO UPDATE SET
    (s.name, s.address, s.nationkey, s.phone, s.acctbal, s.mktsegment, s.comment);
