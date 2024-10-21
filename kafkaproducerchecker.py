import subprocess
import sys
import json
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as sql_max
from kafka import KafkaProducer

# PostgreSQL connection details
jdbc_url = "jdbc:postgresql://ec2-18-132-73-146.eu-west-2.compute.amazonaws.com:5432/testdb"
connection_properties = {
    "user": "consultants",
    "password": "WelcomeItc@2022",
    "driver": "org.postgresql.Driver"
}

# Kafka configuration
kafka_topic = "past_sales_changes1"
bootstrap_servers = [
    "ip-172-31-13-101.eu-west-2.compute.internal:9092",
    "ip-172-31-3-80.eu-west-2.compute.internal:9092",
    "ip-172-31-5-217.eu-west-2.compute.internal:9092",
    "ip-172-31-9-237.eu-west-2.compute.internal:9092"
]

# Initialize Spark session
spark = SparkSession.builder \
    .appName("RealTimeSalesMonitoring") \
    .config("spark.jars", "/usr/local/lib/postgresql-42.2.18.jar") \
    .getOrCreate()

# Kafka Producer setup
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Function to get the latest changes from the past_sales table
def get_changes(last_checked):
    # Load data from PostgreSQL, limiting to five records
    query = f"(SELECT * FROM past_sales WHERE last_modified > '{last_checked}' ORDER BY last_modified LIMIT 5) AS new_sales"
    df = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)
    return df

# Function to send changes to Kafka
def send_to_kafka(df):
    if df.count() > 0:
        # Convert DataFrame to JSON and send to Kafka
        data = df.toJSON().collect()
        for record in data:
            producer.send(kafka_topic, value=record)
            print(f"Record sent to Kafka: {record}")
        print(f"Sent {df.count()} records to Kafka topic {kafka_topic}")

# Initialize the last_checked time
last_checked = "1970-01-01 00:00:00"

# Polling loop to detect changes
try:
    while True:
        # Get the latest changes since last_checked
        changes_df = get_changes(last_checked)

        # Send the changes to Kafka
        send_to_kafka(changes_df)

        # Update the last_checked timestamp
        if changes_df.count() > 0:
            last_checked = changes_df.agg(sql_max("last_modified")).collect()[0][0]

        # Sleep for a few seconds before the next check
        time.sleep(10)

except KeyboardInterrupt:
    print("Stopped by user")

finally:
    # Clean up resources
    producer.close()
    spark.stop()

