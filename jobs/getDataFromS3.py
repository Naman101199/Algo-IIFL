from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, window, min, max, first, last, sum, from_unixtime, date_format
from pyspark.sql import functions as F
from datetime import datetime
import sys
import os
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.Connect import XTSConnect
from utils.config import configuration
from utils.getOHLC import ohlcDataFrame

todays_date = str(datetime.today().date()).replace('-','_')
# Market OHLC
API_KEY = configuration['API_KEY']
API_SECRET = configuration['API_SECRET']
source = "WEBAPI"

xt = XTSConnect(API_KEY, API_SECRET, source)
response = xt.marketdata_login()

exchangeSegment = xt.EXCHANGE_NSEFO
exchangeInstrumentIDs = ['35089', '35415']
startTime = 'Aug 8 2024 091500'
endTime = 'Aug 8 2024 153000'
compressionValue = 60

api_ohlc = ohlcDataFrame(xt, exchangeSegment, exchangeInstrumentIDs, startTime, endTime, compressionValue)
print(api_ohlc.head())
print(api_ohlc.info())

directory = f'/home/ec2-user/Algo-IIFL/data/{todays_date}'
os.makedirs(directory, exist_ok=True)
file_path = os.path.join(directory, 'api_ohlc.csv')

api_ohlc.to_csv(file_path, index=False)


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


# S3 bucket and prefix
bucket_name = "algo-iifl-mumbai"
# todays_date = str(datetime.today().date()).replace('-','_')
prefix = f"data/message1512_json_full/{todays_date}"

# Create the S3 path
s3_path = f"s3a://{bucket_name}/{prefix}"

# Read all parquet files from the S3 path
s3_ohlc = spark.read.parquet(s3_path)

# Cleaning timestamp
# s3_ohlc = s3_ohlc.withColumn("timestamp", from_unixtime(col("LastUpdateTime")))
# s3_ohlc = s3_ohlc.withColumn("timestamp", date_format(col("timestamp"), "HH:mm:ss"))
s3_ohlc = s3_ohlc.withColumn("timestamp", F.from_unixtime(F.col("LastUpdateTime") + 315532800))
s3_ohlc = s3_ohlc.withColumn("timestamp", F.date_trunc('minute', F.col("timestamp")))
s3_ohlc = s3_ohlc.withColumn("timestamp", F.date_format(F.col("timestamp"), "HH:mm:ss"))
# print(s3_ohlc.head())

# Group data by 1-minute intervals and calculate OHLC values
s3_ohlc = s3_ohlc.groupBy(
    col("ExchangeSegment"),
    col("ExchangeInstrumentID"),
    window(col("timestamp"), "1 minute")) \
    .agg(
        first("LastTradedPrice").alias("Open"),
        max("LastTradedPrice").alias("High"),
        min("LastTradedPrice").alias("Low"),
        last("LastTradedPrice").alias("Close"),
        sum("LastTradedQunatity").alias("Volume")
    ) \
    .select(
        col("ExchangeSegment"),
        col("ExchangeInstrumentID").alias("exchangeInstrumentID"),
        col("window.start").alias("start_time"),
        col("window.end").alias("end_time"),
        "Open", "High", "Low", "Close", "Volume"
    )

# Select and reorder columns to match the required format
s3_ohlc = s3_ohlc.withColumn('Timestamp', date_format(col('end_time'), 'HH:mm'))
s3_ohlc = s3_ohlc.select("exchangeInstrumentID", "Timestamp", "Open", "High", "Low", "Close", "Volume")

# Show the dataframe schema and a few rows
s3_ohlc.printSchema()
# s3_ohlc = s3_ohlc.toPandas()
columns = ["exchangeInstrumentID", "Timestamp", "Open", "High", "Low", "Close", "Volume"]
s3_ohlc = pd.DataFrame.from_records(s3_ohlc.collect(), columns=columns)
print(s3_ohlc.head())

file_path = os.path.join(directory, 's3_ohlc.csv')
s3_ohlc.to_csv(file_path, index = False)