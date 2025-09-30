SELECT cp.customer_aggregate_identifier
      ,co.order_aggregate_identifier
      ,cp.custkey
      ,cp.name
      ,cp.address
      ,cp.country
      ,cp.phone
      ,cp.account_balance
      ,cp.total_purchase_order
      ,cp.total_net_lineitem
      ,cp.total_gross_lineitem
      ,cp.total_discount_lineitem
      ,cp.total_tax_lineitem
      ,cp.market_segment
      ,co.orderstatus
      ,co.totalprice
      ,co.orderdate
      ,co.orderpriority
      ,co.clerk
      ,co.shippriority
  FROM staging.customer_profile cp 
  LEFT join staging.customer_orders co 
    ON cp.customer_aggregate_identifier = co.customer_aggregate_identifier
