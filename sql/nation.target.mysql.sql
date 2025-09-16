INSERT INTO db.nation (nationkey, name, regionkey, comment) 
SELECT nationkey, name, regionkey, comment
FROM staging.nation s
ON DUPLICATE KEY UPDATE
nationkey = s.nationkey, name = s.name, regionkey = s.regionkey, comment = s.comment;
