from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, window, min, max, first, last, sum, from_unixtime, date_format
from pyspark.sql import functions as F
from datetime import datetime
import sys
import os
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.config import configuration

# S3 OHLC
spark = SparkSession.builder \
    .appName('SparkDataStreaming') \
    .config('spark.jars.packages', 
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
    "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.0,"
    "org.apache.hadoop:hadoop-aws:3.3.1,"
    "com.amazonaws:aws-java-sdk-bundle:1.11.1026") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", configuration.get("AWS_ACCESS_KEY"))\
    .config("spark.hadoop.fs.s3a.secret.key", configuration.get("AWS_SECRET_KEY"))\
    .config("spark.hadoop.fs.s3a.credentials.provider", "org.apache.hadoop.f3.s3a.impl.SimpleAWSCredentialsProvider")\
    .getOrCreate()

# Create the S3 path
s3_path = f"s3a://algo-kite/data/kite/2024_08_14/part-00000-b3fcc3db-3f12-430b-9d7b-4d09eb5fba8c-c000.snappy.parquet"

# Read all parquet files from the S3 path
s3_ohlc = spark.read.parquet(s3_path)
s3_ohlc.show()