INSERT INTO db.part (partkey, name, mfgr, brand, type, size, container, retailprice, comment)
SELECT partkey, name, mfgr, brand, type, size, container, retailprice, comment
FROM staging.part s
ON DUPLICATE KEY UPDATE
partkey = s.partkey, name = s.name, mfgr = s.mfgr, brand = s.brand, type = s.type, size = s.size, container = s.container, retailprice = s.retailprice, comment = s.comment
