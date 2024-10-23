from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, col as pyspark_col
import subprocess

# Create Spark session with PostgreSQL JDBC configuration
spark = SparkSession.builder \
    .appName("PostgresToHDFS") \
    .master("local[*]") \
    .config("spark.jars", "/usr/local/lib/postgresql-42.2.18.jar") \
    .getOrCreate()

# PostgreSQL connection details
jdbc_url = "jdbc:postgresql://ec2-18-132-73-146.eu-west-2.compute.amazonaws.com:5432/testdb"
connection_properties = {
    "user": "consultants",
    "password": "WelcomeItc@2022",
    "driver": "org.postgresql.Driver"
}

# Function to delete existing files in HDFS if they already exist
def delete_hdfs_path(hdfs_path):
    try:
        # Use subprocess.call for compatibility with older Python versions
        subprocess.call(["hadoop", "fs", "-rm", "-r", hdfs_path])
        print(f"Deleted existing files at {hdfs_path}")
    except subprocess.CalledProcessError:
        print(f"No existing files found at {hdfs_path} to delete.")

# Function to convert date columns to string format
def convert_dates(df):
    date_columns = [col_name for col_name in df.columns if "date" in col_name.lower() or "last_modified" in col_name.lower()]
    for date_col in date_columns:
        df = df.withColumn(date_col, date_format(pyspark_col(date_col), "yyyy-MM-dd"))
    return df

# Function to load data from PostgreSQL and save to HDFS in CSV and Parquet formats
def load_and_save_to_hdfs(table_name, hdfs_path_base):
    try:
        # Load data from PostgreSQL
        print(f"Loading data from PostgreSQL table: {table_name}")
        df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
        
        # Show number of records
        record_count = df.count()
        print(f"Number of records in {table_name}: {record_count}")
        
        # Convert date columns to readable format
        df = convert_dates(df)
        
        # Define paths for CSV and Parquet
        hdfs_path_csv = f"{hdfs_path_base}/{table_name}/csv"
        hdfs_path_parquet = f"{hdfs_path_base}/{table_name}/parquet"
        
        # Delete existing paths if present
        delete_hdfs_path(hdfs_path_csv)
        delete_hdfs_path(hdfs_path_parquet)
        
        # Save to HDFS as CSV
        df.write.mode("overwrite") \
            .option("header", "true") \
            .csv(hdfs_path_csv)
        print(f"Data for {table_name} written to {hdfs_path_csv} in CSV format")
        
        # Save to HDFS as Parquet
        df.write.mode("overwrite") \
            .parquet(hdfs_path_parquet)
        print(f"Data for {table_name} written to {hdfs_path_parquet} in Parquet format")
        
    except Exception as e:
        print(f"[ERROR] Failed to load or save data for table {table_name}. Error: {e}")

# Define HDFS base path
hdfs_path_base = "hdfs://ip-172-31-3-80.eu-west-2.compute.internal:8022/tmp/david/full_load"

# List of tables to process
tables = ["features", "store", "past_sales", "new_sales"]

# Loop through each table and save it to HDFS
for table in tables:
    load_and_save_to_hdfs(table, hdfs_path_base)

# Stop the Spark session
spark.stop()
