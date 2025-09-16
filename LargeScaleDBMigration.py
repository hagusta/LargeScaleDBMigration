import logging
import sys
from configparser import ConfigParser
from pathlib import Path
from typing import List

import jaydebeapi  # To execute raw SQL with JDBC
from pyspark.sql import SparkSession
import argparse

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_migration.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class LargeScaleDBMigration:
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

    def __init__(self,cfg:ConfigParser):
        self.SOURCE_DB_SPARK_HOST = cfg.get("config","SOURCE_DB_SPARK_HOST")
        self.SOURCE_DB_SPARK_PORT = cfg.get("config","SOURCE_DB_SPARK_PORT")
        self.SOURCE_DB_SPARK_DBNAME = cfg.get("config","SOURCE_DB_SPARK_DBNAME")
        self.SOURCE_DB_SPARK_USER = cfg.get("config","SOURCE_DB_SPARK_USER")
        self.SOURCE_DB_SPARK_PASSWORD = cfg.get("config","SOURCE_DB_SPARK_PASSWORD") 
        self.SOURCE_DB_SPARK_TABLE = cfg.get("config","SOURCE_DB_SPARK_TABLE")
        self.SOURCE_DB_SPARK_COLUMN_EXCLUDE = [] 
        self.SOURCE_DB_SPARK_PROTOCOL = cfg.get("config","SOURCE_DB_SPARK_PROTOCOL")
        self.SOURCE_DB_SPARK_DRIVER = cfg.get("config","SOURCE_DB_SPARK_DRIVER")
        self.SOURCE_PK_COLUMN = cfg.get("config","SOURCE_PK_COLUMN")

        self.STAGING_DB_SPARK_HOST = cfg.get("config","STAGING_DB_SPARK_HOST")
        self.STAGING_DB_SPARK_PORT = cfg.get("config","STAGING_DB_SPARK_PORT")
        self.STAGING_DB_SPARK_DBNAME = cfg.get("config","STAGING_DB_SPARK_DBNAME")
        self.STAGING_DB_SPARK_USER = cfg.get("config","STAGING_DB_SPARK_USER")
        self.STAGING_DB_SPARK_PASSWORD = cfg.get("config","STAGING_DB_SPARK_PASSWORD")
        self.STAGING_DB_SPARK_TABLE = cfg.get("config","STAGING_DB_SPARK_TABLE")
        self.STAGING_DB_SPARK_COLUMN_EXCLUDE:List = [] 
        self.STAGING_DB_SPARK_PROTOCOL = cfg.get("config","STAGING_DB_SPARK_PROTOCOL")
        self.STAGING_DB_SPARK_DRIVER = cfg.get("config","STAGING_DB_SPARK_DRIVER")

        self.TARGET_DB_SPARK_HOST = cfg.get("config","TARGET_DB_SPARK_HOST")
        self.TARGET_DB_SPARK_PORT = cfg.get("config","TARGET_DB_SPARK_PORT")
        self.TARGET_DB_SPARK_DBNAME = cfg.get("config","TARGET_DB_SPARK_DBNAME")
        self.TARGET_DB_SPARK_USER = cfg.get("config","TARGET_DB_SPARK_USER")
        self.TARGET_DB_SPARK_PASSWORD = cfg.get("config","TARGET_DB_SPARK_PASSWORD")
        self.TARGET_DB_SPARK_TABLE = cfg.get("config","TARGET_DB_SPARK_TABLE")
        self.TARGET_DB_SPARK_COLUMN_EXCLUDE = []  
        self.TARGET_DB_SPARK_PROTOCOL = cfg.get("config","TARGET_DB_SPARK_PROTOCOL")
        self.TARGET_DB_SPARK_DRIVER = cfg.get("config","TARGET_DB_SPARK_DRIVER")

        self.SOURCE_JDBC_DRIVER_PATH = cfg.get("config","SOURCE_JDBC_DRIVER_PATH")
        self.TARGET_JDBC_DRIVER_PATH = cfg.get("config","TARGET_JDBC_DRIVER_PATH")
        
        self.READ_PARTITIONS = cfg.getint("config","READ_PARTITIONS")
        self.WRITE_THROTTLE_PARTITIONS = cfg.getint("config","WRITE_THROTTLE_PARTITIONS")
        self.JDBC_BATCH_SIZE = cfg.getint("config","JDBC_BATCH_SIZE")
        self.SPARK_MASTER_URL = cfg.get("config","SPARK_MASTER_URL")
        self.JARS_PATHS = [self.SOURCE_JDBC_DRIVER_PATH,self.TARGET_JDBC_DRIVER_PATH]

        print(self.JARS_PATHS)

# --- Spark Session Initialization ---
    def create_spark_session(self, spark_master_url: str, jars_paths: list) -> SparkSession:
        """Initializes and returns a SparkSession with necessary JDBC drivers and custom configs."""
        print("Initializing Spark Session...")
        spark = (SparkSession.builder \
            .appName(f"LargeScaleDBMigration") \
            .master(spark_master_url) \
            .config("log.level","DEBUG") \
            .config("spark.jars", ",".join(jars_paths)) \
            .config("spark.sql.shuffle.partitions", str(self.READ_PARTITIONS * 2)) \
            .config("spark.ui.showConsoleProgress", True)
            .getOrCreate()
                 )
        print("Spark Session created successfully.")
        return spark
        
# --- Helper Function: Get Min/Max PK for Source Partitioning ---
    def get_source_pk_bounds(self,spark_session: SparkSession, sql: str):
        """
        Dynamically fetches the minimum and maximum primary key values from the source table.
        This is essential for robust partitioning of large tables.
        """
        jdbc_url = "jdbc:"+ self.SOURCE_DB_SPARK_PROTOCOL + \
                "://" + self.SOURCE_DB_SPARK_HOST + ":" + \
                self.SOURCE_DB_SPARK_PORT + "/" + self.SOURCE_DB_SPARK_DBNAME
        sqlstate=f"(SELECT MIN({self.SOURCE_PK_COLUMN}) AS min_pk, MAX({self.SOURCE_PK_COLUMN}) AS max_pk FROM ({sql}) AS blah) AS bounds"
    #    print(sqlstate)
        print(f"Determining min/max for {self.SOURCE_PK_COLUMN} in {self.SOURCE_DB_SPARK_TABLE}...")
        try:
            # Use a subquery to fetch min/max to avoid loading the entire table metadata
            min_max_df = spark_session.read \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", sqlstate) \
                .option("user", self.SOURCE_DB_SPARK_USER) \
                .option("password", self.SOURCE_DB_SPARK_PASSWORD) \
                .option("driver",self.SOURCE_DB_SPARK_DRIVER) \
                .load()

            # Check if the DataFrame is empty before calling .first()
            if min_max_df.count() == 0:
                print(f"Warning: Source table {self.SOURCE_DB_SPARK_TABLE} appears to be empty. No data will be migrated.")
                return None, None
                
            min_pk, max_pk = min_max_df.first()
            print(f"Min {self.SOURCE_PK_COLUMN}: {min_pk}, Max {self.SOURCE_PK_COLUMN}: {max_pk}")
            return min_pk, max_pk
        except Exception as e:
            print(f"Error determining PK bounds for {sql}: {e}", file=sys.stderr)
            raise
           
    # --- 1. Data Extraction Function (from PostgreSQL) ---
    def extract_data(self,spark_session: SparkSession, table_name: str, pk_lower_bound: int, pk_upper_bound: int):
        """
        Extracts data from the PostgreSQL source database.
        Uses partitioning based on primary key to parallelize reads for large tables.
        """
        print(f"Extracting data from source table: {self.STAGING_DB_SPARK_TABLE}")
        jdbc_url = "jdbc:"+ self.SOURCE_DB_SPARK_PROTOCOL + \
                "://" + self.SOURCE_DB_SPARK_HOST + ":" + \
                self.SOURCE_DB_SPARK_PORT + "/" + self.SOURCE_DB_SPARK_DBNAME
        try:
            df = spark_session.read \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", f"({table_name}) AS subquery") \
                .option("user", self.SOURCE_DB_SPARK_USER) \
                .option("password", self.SOURCE_DB_SPARK_PASSWORD) \
                .option("driver","org.postgresql.Driver") \
                .option("partitionColumn", self.SOURCE_PK_COLUMN) \
                .option("lowerBound", pk_lower_bound) \
                .option("upperBound", pk_upper_bound) \
                .option("numPartitions", self.READ_PARTITIONS) \
                .load()
            print(f"Schema of extracted data from {table_name}:")
            df.printSchema()
            return df
        except Exception as e:
            print(f"Error during data extraction from {table_name}: {e}", file=sys.stderr)
            raise
            
# --- 2. Data Transformation Function ---
    def transform_data(self,source_df):
        print("Applying data transformations...")
        return source_df
        
 # --- 3. Data Loading Function (to MySQL with Throttling and Staging) ---
    def load_to_staging(self,spark_session: SparkSession, df_to_load, staging_table: str):
        """
        Loads data to the MySQL target database using a staging table for robustness
        and throttles writes by controlling the number of partitions.
        """
        print(f"Starting data load to staging table '{staging_table}'...")

        # Define MySQL JDBC properties
        jdbc_url = f"jdbc:{self.STAGING_DB_SPARK_PROTOCOL}://\
                           {self.STAGING_DB_SPARK_HOST}:{self.STAGING_DB_SPARK_PORT}/{self.STAGING_DB_SPARK_DBNAME}"
        connection_properties = {
            "user": self.STAGING_DB_SPARK_USER,
            "password": self.STAGING_DB_SPARK_PASSWORD,
            "driver": self.STAGING_DB_SPARK_DRIVER,
            "batchsize": str(self.JDBC_BATCH_SIZE),
        }
        if self.STAGING_DB_SPARK_PROTOCOL == "mysql":
            connection_properties["rewriteBatchedStatements"] = "true"
        
        # Step 1: Repartition the DataFrame for throttling
        # This controls the number of concurrent write tasks hitting MySQL.
        print(f"Repartitioning DataFrame to {self.WRITE_THROTTLE_PARTITIONS} partitions for throttling...")
        throttled_df = df_to_load.repartition(self.WRITE_THROTTLE_PARTITIONS)
        print(f"DataFrame now has {throttled_df.rdd.getNumPartitions()} partitions.")

        # Step 2: Write to the temporary staging table in MySQL
        # Using 'overwrite' mode for the staging table ensures it's clean for each run/chunk.
        # Staging table should ideally have minimal/no indexes for fast writes.
        print(f"Writing data to staging table: {staging_table}")
        try:
            throttled_df.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", staging_table) \
                .options(**connection_properties) \
                .mode("append") \
                .save()
            print(f"Successfully wrote {throttled_df.count()} rows to staging table '{staging_table}'.")
        except Exception as e:
            print(f"Error writing to staging table '{staging_table}': {e}", file=sys.stderr)
            raise

# --- Helper Function to execute SQL directly via JDBC ---
    def execute_sql_with_jdbc(self,config: dict,sql_statement: str):
        """
        Executes a given SQL statement on the target database using the driver.
        This is a more robust way to run DML statements like MERGE.
        """
        #jdbc_url = f"jdbc:{self.TARGET_DB_SPARK_PROTOCOL}://{self.TARGET_DB_SPARK_HOST}:{self.TARGET_DB_SPARK_PORT}/{self.TARGET_DB_SPARK_DBNAME}"
        conn = None
        try:
            # CORRECTED: 'ars' to 'jars' AND passing the list from config.
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

    def merge_to_target(self,spark_session: SparkSession,merge_sql:str):
        # Step 3: Merge data from staging table to the final target table using SQL UPSERT/MERGE
        # This ensures data is inserted or updated in the final indexed table.
        # This operation is handled by the database itself, leveraging its indexes.
        # IMPORTANT: The SQL syntax for UPSERT/MERGE varies significantly between databases.
        # You MUST adjust this SQL statement based on your target database type and schema.
        # The example below is for MySQL.
        
        # Dynamically build column list for INSERT and UPDATE parts
        # Exclude the primary key for the UPDATE part if it's not updated
        jdbc_url = f"jdbc:{self.TARGET_DB_SPARK_PROTOCOL}://{self.TARGET_DB_SPARK_HOST}:{self.TARGET_DB_SPARK_PORT}/{self.TARGET_DB_SPARK_DBNAME}"

        config = {
                "jclassname":self.TARGET_DB_SPARK_DRIVER,
                "url":jdbc_url,
                "driver_args":{"user": self.TARGET_DB_SPARK_USER, "password": self.TARGET_DB_SPARK_PASSWORD},
                "jars":[self.TARGET_JDBC_DRIVER_PATH], # CORRECTED: Ensure this is a list.
        }
        
        spark_session.sparkContext.parallelize([merge_sql], 1).foreachPartition(
            lambda partition: self.execute_sql_with_jdbc(config,list(partition)[0])
        )
        

# --- Main Execution Flow ---
if __name__ == "__main__":
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

    mig = LargeScaleDBMigration(cfg)
    spark = mig.create_spark_session(spark_master_url=mig.SPARK_MASTER_URL,jars_paths=mig.JARS_PATHS)

    staging_filename = Path(__file__).parent / "sql" / f"{mig.TARGET_DB_SPARK_TABLE}.staging.sql"
    with open(staging_filename) as f: staging_sql=f.read()
    bmin,bmax = mig.get_source_pk_bounds(spark_session=spark,sql=staging_sql)
    df=mig.extract_data(spark,staging_sql,pk_lower_bound=bmin,pk_upper_bound=bmax) 
    mig.load_to_staging(spark_session=spark,df_to_load=df,staging_table=mig.STAGING_DB_SPARK_TABLE)
    
    target_filename = Path(__file__).parent / "sql" / f"{mig.TARGET_DB_SPARK_TABLE}.target.{mig.TARGET_DB_SPARK_PROTOCOL}.sql"
    with open(target_filename) as f: merge_sql=f.read()
    mig.merge_to_target(spark,merge_sql)
