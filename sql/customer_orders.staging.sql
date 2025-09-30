SELECT o.o_orderkey AS orderkey
      ,decode(md5(o.o_orderkey::bit(64)::text), 'hex') AS order_aggregate_identifier
      ,decode(md5(o.o_custkey::bit(64)::text), 'hex') AS customer_aggregate_identifier
	    ,o.o_orderstatus AS orderstatus
      ,o.o_totalprice::numeric AS totalprice 
      ,o.o_orderdate AS orderdate
      ,o.o_orderpriority AS orderpriority
      ,o.o_clerk AS clerk
      ,o.o_shippriority AS shippriority
      ,o.o_comment AS comment
  FROM tpch.orders o
