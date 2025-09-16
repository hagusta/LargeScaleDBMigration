INSERT INTO db.supplier (suppkey, name, address, nationkey, phone, acctbal, comment)
SELECT suppkey, name, address, nationkey, phone, acctbal, comment 
FROM staging.supplier s
ON DUPLICATE KEY UPDATE
suppkey = s.suppkey, name = s.name, address = s.address, nationkey = s.nationkey, phone = s.phone, acctbal = s.acctbal, comment = s.comment

