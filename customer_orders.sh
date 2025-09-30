#!/bin/bash 
HOME=/home/hagusta/workspace/LargeScaleDBMigration
TABLE_NAME=customer_orders

cd $HOME

#tar -czvf build/${TABLE_NAME}.tar.gz config/${TABLE_NAME}.ini sql/${TABLE_NAME}*

cp LargeScaleDBMigration.py ../spark-docker/spark_apps
#cp build/${TABLE_NAME}.tar.gz ../spark-docker/spark_apps
cp config/${TABLE_NAME}.ini ../spark-docker/spark_apps/config
cp sql/${TABLE_NAME}.* ../spark-docker/spark_apps/sql


docker exec da-spark-master spark-submit --py-files apps/config/${TABLE_NAME}.ini,apps/sql/${TABLE_NAME}.staging.sql,apps/sql/${TABLE_NAME}.target.mysql.sql --master spark://localhost:7077 apps/LargeScaleDBMigration.py -t ${TABLE_NAME}

