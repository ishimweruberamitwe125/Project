from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, col, udf, isnan, when, first, stddev, to_date, count
from pyspark.sql.types import StringType, FloatType, DateType
from pyspark.sql.window import Window

# Initialize Spark session and configure legacy time parsing
spark = SparkSession.builder \
    .appName("DataCleaning") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://ip-172-31-3-80.eu-west-2.compute.internal:8022") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

# Function to load data from HDFS
def load_data(hdfs_path):
    return spark.read.option("header", "true").csv(hdfs_path)

# Function to clean data
def clean_data(df):
    # Initial counts
    print("[INFO] Initial Data Quality Checks:")
    print(f"Number of records: {df.count()}")
    print(f"Number of duplicate records: {df.count() - df.dropDuplicates().count()}")
    
    # Count null values before cleaning
    null_counts = df.select([count(when(col(c).isNull() | (col(c) == "NA") | (col(c) == "na"), c)).alias(c) for c in df.columns])
    print("[INFO] Null Values Count Before Cleaning:")
    null_counts.show()

    # Replace "NA" or "na" with null values
    df = df.replace("NA", None).replace("na", None)
    
    # Remove duplicates
    df = df.dropDuplicates()

    # Handle missing values for numeric columns (int, float)
    numeric_cols = [field.name for field in df.schema.fields if field.dataType in [FloatType(), "IntegerType", "DoubleType"]]
    for col_name in numeric_cols:
        mean_val = df.select(mean(col(col_name))).collect()[0][0]
        df = df.withColumn(col_name, when(col(col_name).isNull(), mean_val).otherwise(col(col_name)))

    # Handle missing values for categorical columns (string)
    string_cols = [field.name for field in df.schema.fields if field.dataType == StringType()]
    for col_name in string_cols:
        mode_val = df.groupBy(col_name).count().orderBy("count", ascending=False).first()[0]
        df = df.withColumn(col_name, when(col(col_name).isNull(), mode_val).otherwise(col(col_name)))

    # Handle missing values in 'Date' column and convert to proper format
    if "Date" in df.columns:
        df = df.withColumn("Date", to_date(col("Date"), "yyyy-MM-dd"))
        # Optionally, if null date needs special handling (forward fill or drop):
        window = Window.orderBy("Date")
        df = df.withColumn("Date", when(col("Date").isNull(), first("Date", True).over(window)).otherwise(col("Date")))

    # String normalization (remove extra spaces, convert to lowercase), excluding datetime columns
    for col_name in string_cols:
        # Skip datetime columns from string normalization
        if df.schema[col_name].dataType != DateType():
            df = df.withColumn(col_name, udf(lambda x: x.strip().lower() if x else None, StringType())(col(col_name)))

    # Check for outliers (Z-Score method, z > 3) and drop them
    outlier_count = 0
    for col_name in numeric_cols:
        stddev_val = df.select(stddev(col(col_name))).collect()[0][0]
        mean_val = df.select(mean(col(col_name))).collect()[0][0]
        outlier_count += df.filter(((col(col_name) - mean_val) / stddev_val).between(-3, 3) == False).count()
        df = df.filter(((col(col_name) - mean_val) / stddev_val).between(-3, 3))

    # Final checks after cleaning
    print("[INFO] Data Quality Checks After Cleaning:")
    print(f"Number of records: {df.count()}")
    print(f"Number of duplicate records: {df.count() - df.dropDuplicates().count()}")
    
    # Count null values after cleaning
    null_counts_after = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
    print("[INFO] Null Values Count After Cleaning:")
    null_counts_after.show()
    
    print(f"[INFO] Number of outliers removed: {outlier_count}")

    return df

# Save the cleaned DataFrame back to HDFS
def save_to_hdfs(df, hdfs_path):
    if df and df.count() > 0:
        # Delete existing files in the HDFS path before saving
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        fs.delete(spark._jvm.org.apache.hadoop.fs.Path(hdfs_path), True)
        print(f"[INFO] Deleted existing files at HDFS path: {hdfs_path}")

        # Save the new records to HDFS
        df.write.mode("overwrite").option("header", "true").csv(hdfs_path)
        print(f"[INFO] Records saved to HDFS at {hdfs_path}")
    else:
        print("[INFO] No new records to save to HDFS.")

# Function to process, clean, and append files
def process_and_clean_file(incremental_path, full_load_path, save_path):
    # Load data from incremental load
    incremental_df = load_data(incremental_path)
    # Clean the incremental data
    cleaned_incremental_df = clean_data(incremental_df)

    # Load data from full load
    full_load_df = load_data(full_load_path)
    
    # Append the cleaned incremental data to the full load data
    appended_df = full_load_df.union(cleaned_incremental_df)

    # Clean the combined data again to ensure no duplicates and consistent formatting
    final_cleaned_df = clean_data(appended_df)

    # Save the cleaned and combined data back to HDFS
    save_to_hdfs(final_cleaned_df, save_path)

# Define paths for HDFS for both incremental load and full load
files_to_clean = [
    {
        "incremental_path": "/tmp/david/incremental_load/new_sales/part-00000-*.csv", 
        "full_load_path": "/tmp/david/full_load/new_sales/part-00000-*.csv", 
        "save_path": "/tmp/david/curated_data/new_sales"
    },
    {
        "incremental_path": "/tmp/david/incremental_load/features/part-00000-*.csv", 
        "full_load_path": "/tmp/david/full_load/features/part-00000-*.csv", 
        "save_path": "/tmp/david/curated_data/features"
    },
    {
        "incremental_path": "/tmp/david/incremental_load/store/part-00000-*.csv", 
        "full_load_path": "/tmp/david/full_load/store/part-00000-*.csv", 
        "save_path": "/tmp/david/curated_data/store"
    },
    {
        "incremental_path": "/tmp/david/incremental_load/past_sales/part-00000-*.csv", 
        "full_load_path": "/tmp/david/full_load/past_sales/part-00000-*.csv", 
        "save_path": "/tmp/david/curated_data/past_sales"
    }
]

# Process each file for cleaning, appending, and saving
for file_info in files_to_clean:
    process_and_clean_file(file_info["incremental_path"], file_info["full_load_path"], file_info["save_path"])

# Stop Spark session
spark.stop()

