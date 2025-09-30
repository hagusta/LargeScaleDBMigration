import sys
from configparser import ConfigParser
from typing import List
from pathlib import Path

import jaydebeapi  # To execute raw SQL with JDBC
from pyspark.sql import SparkSession,DataFrame
import argparse
from transform import *

SOURCE_DB_SPARK_HOST:str
SOURCE_DB_SPARK_PORT:str
SOURCE_DB_SPARK_DBNAME:str
SOURCE_DB_SPARK_USER:str
SOURCE_DB_SPARK_PASSWORD:str
SOURCE_DB_SPARK_TABLE:str
SOURCE_DB_SPARK_COLUMN_EXCLUDE:List = [] 
SOURCE_DB_SPARK_PROTOCOL:str
SOURCE_DB_SPARK_DRIVER:str
SOURCE_PK_COLUMN:str

IS_TRANSFORM:bool = False
TRANSFORM_KEY:str 

STAGING_DB_SPARK_HOST:str
STAGING_DB_SPARK_PORT:str
STAGING_DB_SPARK_DBNAME:str
STAGING_DB_SPARK_USER:str
STAGING_DB_SPARK_PASSWORD:str
STAGING_DB_SPARK_TABLE:str
STAGING_DB_SPARK_COLUMN_EXCLUDE:List = [] 
STAGING_DB_SPARK_PROTOCOL:str
STAGING_DB_SPARK_DRIVER:str

TARGET_DB_SPARK_HOST:str
TARGET_DB_SPARK_PORT:str
TARGET_DB_SPARK_DBNAME:str
TARGET_DB_SPARK_USER:str
TARGET_DB_SPARK_PASSWORD:str
TARGET_DB_SPARK_TABLE:str
TARGET_DB_SPARK_COLUMN_EXCLUDE = [] 
TARGET_DB_SPARK_PROTOCOL:str
TARGET_DB_SPARK_DRIVER:str

SOURCE_JDBC_DRIVER_PATH:str
TARGET_JDBC_DRIVER_PATH:str

# READ_PARTITIONS: Controls the number of parallel reads from the source DB.
# For 2 billion rows and a target chunk size of 1 million, you'd ideally want 2000 partitions.
# However, 20TB of "wide" data might mean 1 million rows is still too large for a single executor's memory.
# Start with a conservative number like 200-500 and increase if your ETL machine has sufficient RAM/cores.
# Each partition will contain (Total Rows / READ_PARTITIONS) rows on average.
READ_PARTITIONS:int = 50

# concurrently without performance degradation (e.g., 5-20).
WRITE_THROTTLE_PARTITIONS:int = 20 

# JDBC_BATCH_SIZE: Number of rows per batch insert operation for JDBC writes.
# A larger batch size reduces network round trips and transaction overhead.
JDBC_BATCH_SIZE:int = 1000 

SPARK_MASTER_URL:str

JARS_PATHS:List = []

def set_Config(cfg:ConfigParser):
    global SOURCE_DB_SPARK_HOST 
    SOURCE_DB_SPARK_HOST = cfg.get("config","SOURCE_DB_SPARK_HOST")
    global SOURCE_DB_SPARK_PORT
    SOURCE_DB_SPARK_PORT = cfg.get("config","SOURCE_DB_SPARK_PORT")
    global SOURCE_DB_SPARK_DBNAME
    SOURCE_DB_SPARK_DBNAME = cfg.get("config","SOURCE_DB_SPARK_DBNAME")
    global SOURCE_DB_SPARK_USER
    SOURCE_DB_SPARK_USER = cfg.get("config","SOURCE_DB_SPARK_USER")
    global SOURCE_DB_SPARK_PASSWORD
    SOURCE_DB_SPARK_PASSWORD = cfg.get("config","SOURCE_DB_SPARK_PASSWORD") 
    global SOURCE_DB_SPARK_TABLE
    SOURCE_DB_SPARK_TABLE = cfg.get("config","SOURCE_DB_SPARK_TABLE")
    global SOURCE_DB_SPARK_PROTOCOL
    SOURCE_DB_SPARK_PROTOCOL = cfg.get("config","SOURCE_DB_SPARK_PROTOCOL")
    global SOURCE_DB_SPARK_DRIVER
    SOURCE_DB_SPARK_DRIVER = cfg.get("config","SOURCE_DB_SPARK_DRIVER")
    global SOURCE_PK_COLUMN
    SOURCE_PK_COLUMN = cfg.get("config","SOURCE_PK_COLUMN")

    global IS_TRANSFORM
    IS_TRANSFORM = cfg.getboolean("config","IS_TRANSFORM")
    global TRANSFORM_KEY
    TRANSFORM_KEY = cfg.get("config","TRANSFORM_KEY")

    global STAGING_DB_SPARK_HOST
    STAGING_DB_SPARK_HOST = cfg.get("config","STAGING_DB_SPARK_HOST")
    global STAGING_DB_SPARK_PORT
    STAGING_DB_SPARK_PORT = cfg.get("config","STAGING_DB_SPARK_PORT")
    global STAGING_DB_SPARK_DBNAME
    STAGING_DB_SPARK_DBNAME = cfg.get("config","STAGING_DB_SPARK_DBNAME")
    global STAGING_DB_SPARK_USER
    STAGING_DB_SPARK_USER = cfg.get("config","STAGING_DB_SPARK_USER")
    global STAGING_DB_SPARK_PASSWORD
    STAGING_DB_SPARK_PASSWORD = cfg.get("config","STAGING_DB_SPARK_PASSWORD")
    global STAGING_DB_SPARK_TABLE
    STAGING_DB_SPARK_TABLE = cfg.get("config","STAGING_DB_SPARK_TABLE")
    global STAGING_DB_SPARK_PROTOCOL
    STAGING_DB_SPARK_PROTOCOL = cfg.get("config","STAGING_DB_SPARK_PROTOCOL")
    global STAGING_DB_SPARK_DRIVER
    STAGING_DB_SPARK_DRIVER = cfg.get("config","STAGING_DB_SPARK_DRIVER")

    global TARGET_DB_SPARK_HOST
    TARGET_DB_SPARK_HOST = cfg.get("config","TARGET_DB_SPARK_HOST")
    global TARGET_DB_SPARK_PORT
    TARGET_DB_SPARK_PORT = cfg.get("config","TARGET_DB_SPARK_PORT")
    global TARGET_DB_SPARK_DBNAME
    TARGET_DB_SPARK_DBNAME = cfg.get("config","TARGET_DB_SPARK_DBNAME")
    global TARGET_DB_SPARK_USER
    TARGET_DB_SPARK_USER = cfg.get("config","TARGET_DB_SPARK_USER")
    global TARGET_DB_SPARK_PASSWORD
    TARGET_DB_SPARK_PASSWORD = cfg.get("config","TARGET_DB_SPARK_PASSWORD")
    global TARGET_DB_SPARK_TABLE
    TARGET_DB_SPARK_TABLE = cfg.get("config","TARGET_DB_SPARK_TABLE")
    global TARGET_DB_SPARK_PROTOCOL
    TARGET_DB_SPARK_PROTOCOL = cfg.get("config","TARGET_DB_SPARK_PROTOCOL")
    global TARGET_DB_SPARK_DRIVER
    TARGET_DB_SPARK_DRIVER = cfg.get("config","TARGET_DB_SPARK_DRIVER")

    global SOURCE_JDBC_DRIVER_PATH
    SOURCE_JDBC_DRIVER_PATH = cfg.get("config","SOURCE_JDBC_DRIVER_PATH")
    global TARGET_JDBC_DRIVER_PATH
    TARGET_JDBC_DRIVER_PATH = cfg.get("config","TARGET_JDBC_DRIVER_PATH")
    
    global READ_PARTITIONS
    READ_PARTITIONS = cfg.getint("config","READ_PARTITIONS")
    global WRITE_THROTTLE_PARTITIONS
    WRITE_THROTTLE_PARTITIONS = cfg.getint("config","WRITE_THROTTLE_PARTITIONS")
    global JDBC_BATCH_SIZE
    JDBC_BATCH_SIZE = cfg.getint("config","JDBC_BATCH_SIZE")
    global SPARK_MASTER_URL
    SPARK_MASTER_URL = cfg.get("config","SPARK_MASTER_URL")
    global JARS_PATHS
    JARS_PATHS = [SOURCE_JDBC_DRIVER_PATH,TARGET_JDBC_DRIVER_PATH]

# --- Spark Session Initialization ---
def create_spark_session( spark_master_url: str, jars_paths: list) -> SparkSession:
    """Initializes and returns a SparkSession with necessary JDBC drivers and custom configs."""
    print("\nInitializing Spark Session...\n")
    spark = (SparkSession.builder \
        .appName(f"LargeScaleDBMigration") \
        .master(spark_master_url) \
        .config("log.level","DEBUG") \
        .config("spark.jars", ",".join(jars_paths)) \
        .config("spark.sql.shuffle.partitions", str(READ_PARTITIONS * 2)) \
        .config("spark.ui.showConsoleProgress", True)
        .getOrCreate()
             )
    print("\nSpark Session created successfully.\n")
    return spark
    
# --- Helper Function: Get Min/Max PK for Source Partitioning ---
def get_source_pk_bounds(spark_session: SparkSession, sql: str):
    """
    Dynamically fetches the minimum and maximum primary key values from the source table.
    This is essential for robust partitioning of large tables.
    """
    jdbc_url = "jdbc:"+ SOURCE_DB_SPARK_PROTOCOL + \
            "://" + SOURCE_DB_SPARK_HOST + ":" + \
            SOURCE_DB_SPARK_PORT + "/" + SOURCE_DB_SPARK_DBNAME
    sqlstate=f"(SELECT MIN({SOURCE_PK_COLUMN}) AS min_pk, MAX({SOURCE_PK_COLUMN}) AS max_pk FROM ({sql}) AS blah) AS bounds"
#    print(sqlstate)
    print(f"\nDetermining min/max for {SOURCE_PK_COLUMN} in {SOURCE_DB_SPARK_TABLE}...\n")
    try:
        # Use a subquery to fetch min/max to avoid loading the entire table metadata
        min_max_df = spark_session.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", sqlstate) \
            .option("user", SOURCE_DB_SPARK_USER) \
            .option("password", SOURCE_DB_SPARK_PASSWORD) \
            .option("driver",SOURCE_DB_SPARK_DRIVER) \
            .load()

        # Check if the DataFrame is empty before calling .first()
        if min_max_df.count() == 0:
            print(f"Warning: Source table {SOURCE_DB_SPARK_TABLE} appears to be empty. No data will be migrated.")
            return 0, 0
            
        min_pk, max_pk = min_max_df.first()
        print(f"\nMin {SOURCE_PK_COLUMN}: {min_pk}, Max {SOURCE_PK_COLUMN}: {max_pk}\n")
        return min_pk, max_pk
    except Exception as e:
        print(f"Error determining PK bounds for {sql}: {e}", file=sys.stderr)
        raise
       
# --- 1. Data Extraction Function (from PostgreSQL) ---
def extract_data(spark_session: SparkSession, query: str, pk_lower_bound: int, pk_upper_bound: int) -> DataFrame:
    """
    Extracts data from the PostgreSQL source database.
    Uses partitioning based on primary key to parallelize reads for large tables.
    """
    print(f"\nExtracting data from source table: {STAGING_DB_SPARK_TABLE}\n")
    jdbc_url = "jdbc:"+ SOURCE_DB_SPARK_PROTOCOL + \
            "://" + SOURCE_DB_SPARK_HOST + ":" + \
            SOURCE_DB_SPARK_PORT + "/" + SOURCE_DB_SPARK_DBNAME
    try:
        df = spark_session.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", f"({query}) AS subquery") \
            .option("user", SOURCE_DB_SPARK_USER) \
            .option("password", SOURCE_DB_SPARK_PASSWORD) \
            .option("driver",SOURCE_DB_SPARK_DRIVER) \
            .option("partitionColumn", SOURCE_PK_COLUMN) \
            .option("lowerBound", pk_lower_bound) \
            .option("upperBound", pk_upper_bound) \
            .option("numPartitions", READ_PARTITIONS) \
            .load()
        #print(f"\nSchema of extracted data from {query}:")
        #df.printSchema()
        return df
    except Exception as e:
        print(f"Error during data extraction from {query}: {e}", file=sys.stderr)
        raise

TRANSFORM_FN={"customer_event":customer_event}

# --- 2. Data Transformation Function ---
def transform_data(source_df:DataFrame) -> DataFrame:
    if not IS_TRANSFORM:
        print(f"\nSkip tranform table: {STAGING_DB_SPARK_TABLE} IS_TRANSFORM: {IS_TRANSFORM}\n")
        return source_df
    print("\nApplying data transformations...\n")
    fn=TRANSFORM_FN[STAGING_DB_SPARK_TABLE]
    repartition_df = source_df.repartition(READ_PARTITIONS,TRANSFORM_KEY)
    result_df = fn(repartition_df)
    return result_df
    
# --- 3. Data Loading Function (to MySQL with Throttling and Staging) ---
def load_to_staging(spark_session: SparkSession, df_to_load, staging_table: str):
    """
    Loads data to the MySQL target database using a staging table for robustness
    and throttles writes by controlling the number of partitions.
    """
    print(f"\nStarting data load to staging table '{staging_table}'...\n")

    # Define MySQL JDBC properties
    jdbc_url = f"jdbc:{STAGING_DB_SPARK_PROTOCOL}://\
                       {STAGING_DB_SPARK_HOST}:{STAGING_DB_SPARK_PORT}/{STAGING_DB_SPARK_DBNAME}"
    connection_properties = {
        "user": STAGING_DB_SPARK_USER,
        "password": STAGING_DB_SPARK_PASSWORD,
        "driver": STAGING_DB_SPARK_DRIVER,
        "batchsize": str(JDBC_BATCH_SIZE),
    }
    if STAGING_DB_SPARK_PROTOCOL == "mysql":
        connection_properties["rewriteBatchedStatements"] = "true"
    
    # Step 1: Repartition the DataFrame for throttling
    # This controls the number of concurrent write tasks hitting MySQL.
    print(f"\nRepartitioning DataFrame to {WRITE_THROTTLE_PARTITIONS} partitions for throttling...\n")
    throttled_df = df_to_load.repartition(WRITE_THROTTLE_PARTITIONS)
    print(f"\nDataFrame now has {throttled_df.rdd.getNumPartitions()} partitions.\n")

    # Step 2: Write to the temporary staging table in MySQL
    # Using 'overwrite' mode for the staging table ensures it's clean for each run/chunk.
    # Staging table should ideally have minimal/no indexes for fast writes.
    print(f"\nWriting data to staging table: {staging_table}\n")
    try:
        throttled_df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", staging_table) \
            .options(**connection_properties) \
            .mode("append") \
            .save()
        print(f"\nSuccessfully wrote {throttled_df.count()} rows to staging table '{staging_table}'.\n")
    except Exception as e:
        print(f"Error writing to staging table '{staging_table}': {e}", file=sys.stderr)
        raise

# --- Helper Function to execute SQL directly via JDBC ---
def execute_sql_with_jdbc(config: dict,sql_statement: str):
    """
    Executes a given SQL statement on the target database using the driver.
    This is a more robust way to run DML statements like MERGE.
    """
    conn = None
    try:
        conn = jaydebeapi.connect(jclassname=config["jclassname"], \
                                  url=config["url"], \
                                  driver_args=config["driver_args"], \
                                  jars=config["jars"]
                                 )
        
        conn.jconn.setAutoCommit(False)
        curs = conn.cursor()
        print(f"Executing SQL statement: {sql_statement}")
        curs.execute(sql_statement)
        conn.commit()
    except Exception as e:
        print(f"Error executing SQL statement: {e}", file=sys.stderr)
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

def merge_to_target(spark_session: SparkSession,merge_sql:str):
    # Step 3: Merge data from staging table to the final target table using SQL UPSERT/MERGE
    # This ensures data is inserted or updated in the final indexed table.
    # This operation is handled by the database it leveraging its indexes.
    # IMPORTANT: The SQL syntax for UPSERT/MERGE varies significantly between databases.
    # You MUST adjust this SQL statement based on your target database type and schema.
    # The example below is for MySQL.
    
    # Dynamically build column list for INSERT and UPDATE parts
    # Exclude the primary key for the UPDATE part if it's not updated
    print(f"\nMerge to target table . . .\n")
    jdbc_url = f"jdbc:{TARGET_DB_SPARK_PROTOCOL}://{TARGET_DB_SPARK_HOST}:{TARGET_DB_SPARK_PORT}/{TARGET_DB_SPARK_DBNAME}"

    config = {
            "jclassname":TARGET_DB_SPARK_DRIVER,
            "url":jdbc_url,
            "driver_args":{"user": TARGET_DB_SPARK_USER, "password": TARGET_DB_SPARK_PASSWORD},
            "jars":[TARGET_JDBC_DRIVER_PATH], # CORRECTED: Ensure this is a list.
    }
    
    spark_session.sparkContext.parallelize([merge_sql], 1).foreachPartition(
        lambda partition: execute_sql_with_jdbc(config,list(partition)[0]))
        
def main():
    bmin:int
    bmax:int

    spark = None 
    parser=argparse.ArgumentParser()
    parser.add_argument("tableName", nargs="?")
    parser.add_argument("--tablename")
    parser.add_argument("-t")
    args=parser.parse_args()
      
    if args.tableName:
        TABLE_NAME=args.tableName
    elif args.tablename:
        TABLE_NAME=args.tablename
    elif args.t:
        TABLE_NAME=args.t
    else:
        TABLE_NAME='customer'
      
    config_filename = Path(__file__).parent / "config" / f"{TABLE_NAME}.ini"

    print(f"Open config file {config_filename} . . .")
    cfg = ConfigParser()
    cfg.read(config_filename)

    set_Config(cfg)
    spark = create_spark_session(spark_master_url=SPARK_MASTER_URL,jars_paths=JARS_PATHS)

    staging_filename = Path(__file__).parent / "sql" / f"{TARGET_DB_SPARK_TABLE}.staging.sql"
    with open(staging_filename) as f: staging_sql=f.read()
    bmin,bmax = get_source_pk_bounds(spark_session=spark,sql=staging_sql)
    df=extract_data(spark,staging_sql,pk_lower_bound=bmin,pk_upper_bound=bmax)
    df=transform_data(df)
    load_to_staging(spark_session=spark,df_to_load=df,staging_table=STAGING_DB_SPARK_TABLE)
    
    target_filename = Path(__file__).parent / "sql" / f"{TARGET_DB_SPARK_TABLE}.target.{TARGET_DB_SPARK_PROTOCOL}.sql"
    with open(target_filename) as f: merge_sql=f.read()
    merge_to_target(spark,merge_sql)

# --- Main Execution Flow ---
if __name__ == "__main__":
    main()
