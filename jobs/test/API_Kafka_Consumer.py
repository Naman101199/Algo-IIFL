from confluent_kafka import Consumer, KafkaError, KafkaException
from pyspark.sql import SparkSession
import json
import os
from config import configuration
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType, TimestampType
import ast

# Set AWS credentials as environment variables
os.environ['AWS_ACCESS_KEY_ID'] = configuration['AWS_ACCESS_KEY']
os.environ['AWS_SECRET_ACCESS_KEY'] = configuration['AWS_SECRET_KEY']

# Kafka consumer configuration
consumer_config = {
    'bootstrap.servers': 'localhost:9092',  # Update with your Kafka broker address
    'group.id': 'market_data_group',  # Consumer group ID
    'auto.offset.reset': 'earliest'  # Start reading at the earliest message
}

# Create a Kafka consumer
consumer = Consumer(consumer_config)

# Subscribe to the topic
topic_name = 'market_data_topic'
consumer.subscribe([topic_name])

# Initialize Spark session
# spark = SparkSession.builder \
#     .appName("KafkaToSparkSQL") \
#     .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0") \
#     .getOrCreate()

spark = SparkSession.builder \
    .appName('SparkDataStreaming') \
    .config('spark.jars.packages', 
    "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1,"
    "org.apache.hadoop:hadoop-aws:3.2.0,"
    "com.amazonaws:aws-java-sdk:1.11.469") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", configuration.get("AWS_ACCESS_KEY"))\
    .config("spark.hadoop.fs.s3a.secret.key", configuration.get("AWS_SECRET_KEY"))\
    .config("spark.hadoop.fs.s3a.credentials.provider", "org.apache.hadoop.f3.s3a.impl.SimpleAWSCredentialProvider")\
    .getOrCreate()


# Define schema for the data if necessary

schema = StructType([
    StructField("t", StringType(), False),        # Exchangesegment_ExchangeInstrumentID(exchange segment enum with underscore along with instumentID of the particular subscribed instrument)
    StructField("ltp", DoubleType(), False),      # Last traded price
    StructField("ltq", LongType(), False),        # Last traded quantity
    StructField("tb", LongType(), False),         # Total buy quantity
    StructField("ts", LongType(), False),         # Total sell quantity
    StructField("v", LongType(), False),          # The volume is commonly reported as the number of shares that changed hands during a given day
    StructField("ap", DoubleType(), False),       # Average Traded Price
    StructField("ltt", TimestampType(), False),   # Last Traded Time
    StructField("lut", TimestampType(), False),   # Last Update Time
    StructField("pc", DoubleType(), False),       # Percent Change
    StructField("o", DoubleType(), False),        # Open
    StructField("h", DoubleType(), False),        # High
    StructField("l", DoubleType(), False),        # Low
    StructField("c", DoubleType(), False),        # Close
    StructField("vp", DoubleType(), False),       # Total price volume
    StructField("ai", StringType(), False),       # Ask Info(Ask Size Index+'|'+Ask Price +'|'+ Ask TotalOrders)
    StructField("bi", StringType(), False)        # Bid Info(Bid Size Index+'|'+ Bid Price +'|'+Bid TotalOrders)
])

def consume_from_kafka():
    try:
        while True:
            msg = consumer.poll(timeout=1.0)  # Poll for new messages
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    print(f'End of partition reached {msg.partition()}')
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                # Proper message
                value = msg.value().decode('utf-8')
                print(f'Received message: {value}')
                process_and_write_to_spark(value)

    except KeyboardInterrupt:
        pass
    finally:
        # Close the consumer when done
        consumer.close()

def process_and_write_to_spark(value):
    # value = value.replace("'", "\"")
    value = '{' + value + '}'
    print(value)
    data = ast.literal_eval(json.dumps(value))
    print(data)
    # Convert JSON string to a Python dictionary
    # data = json.loads(value)

    # Create a DataFrame from the dictionary
    df = spark.createDataFrame([data])

    # # Write DataFrame to Spark SQL table
    # df.write.mode('append').saveAsTable('market_data_table')

if __name__ == "__main__":
    consume_from_kafka()