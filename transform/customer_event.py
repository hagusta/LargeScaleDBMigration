import pyspark.sql.functions as F 

customer_profile_columns=[
        'customer_aggregate_identifier','custkey', 'name', 'address', 'country', 'phone', 'account_balance', 'total_purchase_order', 
        'total_net_lineitem', 'total_gross_lineitem', 'total_discount_lineitem', 'total_tax_lineitem', 'market_segment'
        ]

customer_order_columns=['order_aggregate_identifier', 'orderstatus', 'totalprice', 'orderdate', 'orderpriority', 'clerk', 'shippriority']

#target_columns=["customer_aggregate_identifier","event_type","event_timestamp","event_parameters"]

def customer_event(partition_interation):
    df=partition_interation\
        .groupBy(customer_profile_columns)\
        .agg(F.collect_list(F.to_json(F.struct(customer_order_columns)))\
        .alias("customer_order"))
    customer_profile_columns.append("customer_order")
    customer_profile_columns.remove("custkey")
    #print(customer_profile_columns)
    df=df\
        .groupBy("customer_aggregate_identifier","custkey")\
        .agg(F.to_json(F.collect_list(F.to_json(F.struct(customer_profile_columns))))\
        .alias("event_parameters"))
    df=df.withColumn(
            "event_type",F.lit("MigEvent")
            )
    df=df.withColumn(
            "event_timestamp",F.lit(F.unix_timestamp())
            )
    return df
