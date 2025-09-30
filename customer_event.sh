#!/bin/bash 
HOME=/home/hagusta/workspace/LargeScaleDBMigration
TABLE_NAME=customer_event

cd $HOME


cp LargeScaleDBMigration.py ../spark-docker/spark_apps
cp transform/__init__.py ../spark-docker/spark_apps/transform
cp transform/${TABLE_NAME}.py ../spark-docker/spark_apps/transform 
cp config/${TABLE_NAME}.ini ../spark-docker/spark_apps/config
cp sql/${TABLE_NAME}.* ../spark-docker/spark_apps/sql


docker exec da-spark-master spark-submit --py-files apps/config/${TABLE_NAME}.ini,apps/sql/${TABLE_NAME}.staging.sql,apps/sql/${TABLE_NAME}.target.mysql.sql --master spark://localhost:7077 apps/LargeScaleDBMigration.py -t ${TABLE_NAME}

