from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, BooleanType, DateType
from pyspark.sql import functions as F

# Initialize Spark session and enable Hive support with the correct Hive Metastore URI
spark = SparkSession.builder \
    .appName("HiveTableCreation") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://ip-172-31-3-80.eu-west-2.compute.internal:8022") \
    .config("hive.metastore.uris", "thrift://ip-172-31-1-36.eu-west-2.compute.internal:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# Function to create the database if it doesn't exist
def create_database_if_not_exists(db_name):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")

# Drop the table if it exists
def drop_table_if_exists(db_name, table_name):
    spark.sql(f"DROP TABLE IF EXISTS {db_name}.{table_name}")

# Function to create Hive table, load data, and save as CSV in an external location
def create_and_load_hive_table(hdfs_path, table_name, schema, external_csv_path, db_name="sept"):
    # Load data from HDFS with the header option enabled
    df = spark.read.option("header", "true").schema(schema).csv(hdfs_path)
    
    # Drop rows with null values
    df_cleaned = df.dropna()

    # Convert 'last_modified' from string to 'DateType' if exists in the schema
    if 'last_modified' in df_cleaned.columns:
        df_cleaned = df_cleaned.withColumn("last_modified", F.to_date(F.col("last_modified"), "yyyy-MM-dd'T'HH:mm:ss.SSSX"))

    # Drop the table if it exists
    drop_table_if_exists(db_name, table_name)

    # Create the Hive external table with explicit headers
    hive_table_location = f"/tmp/david/hivetable/{table_name}"

    # Write data to Hive as an external table
    df_cleaned.write \
        .mode("overwrite") \
        .format("hive") \
        .option("path", hive_table_location) \
        .saveAsTable(f"{db_name}.{table_name}")

    # Save the table data as a CSV with header to the external location
    external_csv_output = f"{external_csv_path}/{table_name}.csv"
    df_cleaned.write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(external_csv_output)

    # Print the table schema to verify the data types and headers
    spark.sql(f"DESCRIBE {db_name}.{table_name}").show()

# Create the database if it doesn't exist
create_database_if_not_exists("sept")

# Define schemas for each table
new_sales_schema = StructType([
    StructField("store", IntegerType(), True),
    StructField("dept", IntegerType(), True),
    StructField("date", DateType(), True),
    StructField("isholiday", BooleanType(), True),
    StructField("last_modified", StringType(), True)  # To be converted
])

features_schema = StructType([
    StructField("store", IntegerType(), True),
    StructField("date", DateType(), True),
    StructField("temperature", FloatType(), True),
    StructField("fuel_price", FloatType(), True),
    StructField("markdown1", FloatType(), True),
    StructField("markdown2", FloatType(), True),
    StructField("markdown3", FloatType(), True),
    StructField("markdown4", FloatType(), True),
    StructField("markdown5", FloatType(), True),
    StructField("cpi", FloatType(), True),
    StructField("unemployment", FloatType(), True),
    StructField("isholiday", BooleanType(), True),
    StructField("last_modified", StringType(), True)  # To be converted
])

store_schema = StructType([
    StructField("store", IntegerType(), True),
    StructField("type", StringType(), True),
    StructField("size", IntegerType(), True),
    StructField("last_modified", StringType(), True)  # To be converted
])

past_sales_schema = StructType([
    StructField("store", IntegerType(), True),
    StructField("dept", IntegerType(), True),
    StructField("date", DateType(), True),
    StructField("weekly_sales", FloatType(), True),
    StructField("isholiday", BooleanType(), True),
    StructField("last_modified", StringType(), True)  # To be converted
])

# Define the list of files, table names, and corresponding schemas
data_files = [
    {"file_path": "/tmp/david/curated_data/new_sales", "table_name": "new_sales", "schema": new_sales_schema},
    {"file_path": "/tmp/david/curated_data/features", "table_name": "features", "schema": features_schema},
    {"file_path": "/tmp/david/curated_data/store", "table_name": "store", "schema": store_schema},
    {"file_path": "/tmp/david/curated_data/past_sales", "table_name": "past_sales", "schema": past_sales_schema}
]

# External location where CSV files will be saved
external_csv_base_path = "/tmp/david/external_csv"

# Load the data, create Hive tables, and save as CSV to external location
for data in data_files:
    create_and_load_hive_table(data["file_path"], data["table_name"], data["schema"], external_csv_base_path)

# Stop Spark session
spark.stop()

