WITH total_purchase_order AS (
SELECT o.o_custkey  
      ,SUM(o.o_totalprice::numeric) AS total
  FROM tpch.orders o
 GROUP BY o.o_custkey 
)
, total_lineitem AS (
SELECT o.o_custkey
      ,SUM(l.l_extendedprice::numeric - l.l_discount::numeric + l.l_tax::numeric) AS total_net_lineitem
	    ,SUM(l.l_extendedprice::numeric) AS total_gross_lineitem
	    ,SUM(l.l_discount::numeric) AS total_discount_lineitem
	    ,SUM(l.l_tax::numeric) AS total_tax_lineitem
  FROM tpch.lineitem l 
       INNER JOIN tpch.orders o on o.o_orderkey = l.l_orderkey 
 GROUP BY o.o_custkey 
)
SELECT c.c_custkey as custkey
      ,decode(md5(c.c_custkey::bit(64)::text), 'hex') AS customer_aggregate_identifier
      ,c.c_name AS name
      ,c.c_address AS address
      ,n.n_name AS country
      ,c.c_phone AS phone
      ,c.c_acctbal AS account_balance
      ,COALESCE(t.total, 0) AS total_purchase_order
	    ,COALESCE(l.total_net_lineitem, 0) AS total_net_lineitem
	    ,COALESCE(l.total_gross_lineitem, 0) AS total_gross_lineitem
	    ,COALESCE(l.total_discount_lineitem, 0) AS total_discount_lineitem
	    ,COALESCE(l.total_tax_lineitem, 0) AS total_tax_lineitem
      ,c.c_mktsegment AS market_segment
      ,c.c_comment AS comment 
  FROM tpch.customer c
       LEFT JOIN tpch.nation n 
         ON c.c_nationkey = n.n_nationkey 
       LEFT JOIN total_purchase_order t
         ON c.c_custkey = t.o_custkey
	     LEFT JOIN total_lineitem l 
	       ON c.c_custkey = l.o_custkey
