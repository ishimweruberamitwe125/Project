from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, col
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
        subprocess.run(["hadoop", "fs", "-rm", "-r", hdfs_path], check=True)
        print("[INFO] Deleted existing files at {}".format(hdfs_path))
    except subprocess.CalledProcessError:
        print("[INFO] No existing files found at {} to delete.".format(hdfs_path))

# Function to convert date columns to string format
def convert_dates(df):
    date_columns = [col for col in df.columns if "date" in col.lower() or "last_modified" in col.lower()]
    for date_col in date_columns:
        df = df.withColumn(date_col, date_format(col(date_col), "yyyy-MM-dd"))
    return df

# Function to load data from PostgreSQL and save to HDFS in CSV and Parquet formats
def load_and_save_to_hdfs(table_name, hdfs_path_base):
    try:
        # Load data from PostgreSQL
        print("[INFO] Loading data from PostgreSQL table: {}".format(table_name))
        df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
        
        # Show number of records
        record_count = df.count()
        print("[INFO] Number of records in {}: {}".format(table_name, record_count))
        
        # Convert date columns to readable format
        df = convert_dates(df)
        
        # Define paths for CSV and Parquet
        hdfs_path_csv = "{}/{}/csv".format(hdfs_path_base, table_name)
        hdfs_path_parquet = "{}/{}/parquet".format(hdfs_path_base, table_name)
        
        # Delete existing paths if present
        delete_hdfs_path(hdfs_path_csv)
        delete_hdfs_path(hdfs_path_parquet)
        
        # Save to HDFS as CSV
        df.write.mode("overwrite") \
            .option("header", "true") \
            .csv(hdfs_path_csv)
        print("[INFO] Data for {} written to {} in CSV format".format(table_name, hdfs_path_csv))
        
        # Save to HDFS as Parquet
        df.write.mode("overwrite") \
            .parquet(hdfs_path_parquet)
        print("[INFO] Data for {} written to {} in Parquet format".format(table_name, hdfs_path_parquet))
        
    except Exception as e:
        print("[ERROR] Failed to load or save data for table {}. Error: {}".format(table_name, e))

# Define HDFS base path
hdfs_path_base = "hdfs://ip-172-31-3-80.eu-west-2.compute.internal:8022/tmp/david/full_load"

# List of tables to process
tables = ["features", "store", "past_sales", "new_sales"]

# Loop through each table and save it to HDFS
for table in tables:
    load_and_save_to_hdfs(table, hdfs_path_base)

# Stop the Spark session
spark.stop()
