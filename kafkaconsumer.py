from kafka import KafkaConsumer, TopicPartition
import psycopg2
import json

# PostgreSQL connection details
conn = psycopg2.connect(
    host="ec2-18-132-73-146.eu-west-2.compute.amazonaws.com",
    port="5432",
    dbname="testdb",
    user="consultants",
    password="WelcomeItc@2022"
)
cursor = conn.cursor()

# Create table in PostgreSQL if not exists
cursor.execute("""
    CREATE TABLE IF NOT EXISTS sales_changes (
        store_id INT,
        dept INT,
        date DATE,
        weekly_sales FLOAT,
        isholiday BOOLEAN,
        last_modified TIMESTAMP
    );
""")
conn.commit()

# Kafka Consumer configuration
consumer = KafkaConsumer(
    bootstrap_servers=[
        'ip-172-31-13-101.eu-west-2.compute.internal:9092',
        'ip-172-31-3-80.eu-west-2.compute.internal:9092',
        'ip-172-31-5-217.eu-west-2.compute.internal:9092',
        'ip-172-31-9-237.eu-west-2.compute.internal:9092'
    ],
    auto_offset_reset='earliest',  # Start from the earliest message if no offset is committed
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Deserialize value to JSON
)

# Assign the consumer to partition 0 of the topic 'past_sales_changes1'
partition = TopicPartition('past_sales_changes1', 0)
consumer.assign([partition])

# Set the consumer to start from offset 0
consumer.seek(partition, 0)

# Function to insert data into PostgreSQL
def store_to_postgresql(record):
    query = """
        INSERT INTO sales_changes (store_id, dept, date, weekly_sales, isholiday, last_modified)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    cursor.execute(query, (
        record['Store'],  # Use capitalized keys as per the received record
        record['Dept'], 
        record['Date'], 
        record['Weekly_Sales'], 
        record['IsHoliday'], 
        record['last_modified']
    ))
    conn.commit()

# Start consuming messages from Kafka
try:
    for message in consumer:
        record = message.value

        # Ensure the message is properly deserialized (double-check if it's a string)
        if isinstance(record, str):
            record = json.loads(record)  # Explicitly load it as JSON if needed

        print(f"Received record: {record}")

        # Store the record in PostgreSQL
        store_to_postgresql(record)

except KeyboardInterrupt:
    print("Consumer stopped.")

finally:
    # Clean up connections
    cursor.close()
    conn.close()
    consumer.close()

