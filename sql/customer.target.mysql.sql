INSERT INTO db.customer (custkey, name, address, nationkey, phone, acctbal, mktsegment, comment)
SELECT custkey, name, address, nationkey, phone, acctbal, mktsegment, comment
FROM staging.customer s
ON DUPLICATE KEY UPDATE
name=s.name, address=s.address, nationkey=s.nationkey, phone=s.phone, acctbal=s.acctbal, mktsegment=s.mktsegment, comment=s.comment;
