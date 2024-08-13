from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, BooleanType, DoubleType, IntegerType, LongType, StringType, TimestampType
import logging
from datetime import datetime

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.config import configuration

# Configure logging
todays_date = str(datetime.today().date()).replace('-','_')
log_file = f'/home/ec2-user/Algo-IIFL/logs/consumer_kite_{todays_date}.log'
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler(log_file),
                        logging.StreamHandler()
                    ])
logger = logging.getLogger(__name__)

def main():

    topic_name = 'kite'
    checkpoint_folder = f's3a://algo-kite/checkpoints/tick_data/{topic_name}/{todays_date}'
    output_folder = f's3a://algo-kite/data/{topic_name}/{todays_date}'

#     schema = StructType([
#     StructField("tradable", BooleanType(), False),
#     StructField("mode", StringType(), False),
#     StructField("instrument_token", LongType(), False),
#     StructField("last_price", DoubleType(), False),
#     StructField("last_traded_quantity", IntegerType(), False),
#     StructField("average_traded_price", DoubleType(), False),
#     StructField("volume_traded", LongType(), False),
#     StructField("total_buy_quantity", LongType(), False),
#     StructField("total_sell_quantity", LongType(), False),
#     StructField("ohlc", StructType([
#         StructField("open", DoubleType(), False),
#         StructField("high", DoubleType(), False),
#         StructField("low", DoubleType(), False),
#         StructField("close", DoubleType(), False)
#     ]), False),
#     StructField("change", DoubleType(), False),
#     StructField("last_trade_time", TimestampType(), False),
#     StructField("oi", LongType(), False),
#     StructField("oi_day_high", LongType(), False),
#     StructField("oi_day_low", LongType(), False),
#     StructField("exchange_timestamp", TimestampType(), False),
#     StructField("depth", StructType([
#         StructField("buy", StructType([
#             StructField("quantity", IntegerType(), False),
#             StructField("price", DoubleType(), False),
#             StructField("orders", IntegerType(), False)
#         ]), False),
#         StructField("sell", StructType([
#             StructField("quantity", IntegerType(), False),
#             StructField("price", DoubleType(), False),
#             StructField("orders", IntegerType(), False)
#         ]), False)
#     ]), False)
# ])

    schema = StructType([
        StructField("tradable", BooleanType(), True),
        StructField("mode", StringType(), True),
        StructField("instrument_token", LongType(), True),
        StructField("last_price", DoubleType(), True),
        StructField("last_traded_quantity", LongType(), True),
        StructField("average_traded_price", DoubleType(), True),
        StructField("volume_traded", LongType(), True),
        StructField("total_buy_quantity", LongType(), True),
        StructField("total_sell_quantity", LongType(), True),
        StructField("change", DoubleType(), True)
    ])

    try:
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

        spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark connection created successfully!")

    except Exception as e:
        logger.error(f"Couldn't create the spark session due to exception {e}")


    def read_kafka_topic(topic, schema):

        try:
            spark_df = spark.readStream \
                .format('kafka') \
                .option('kafka.bootstrap.servers', '43.205.25.254:9092') \
                .option('subscribe', topic) \
                .option('startingOffsets', 'earliest') \
                .load() 
                # .option('failOnDataLoss', 'false') \

            schema_df = spark_df \
                .selectExpr("CAST(value AS STRING)")\
                .select(from_json(col('value'), schema).alias('data'))\
                .select("data.*")
                # \
                # .withWatermark('timestamp', '5 minutes')

            logging.info("kafka dataframe created successfully")

        except Exception as e:
            schema_df = None
            logging.warning(f"kafka dataframe could not be created because: {e}")

        return schema_df


    def streamWriter(input, checkpointFolder, output):
        logger.info(f"Starting stream writer with checkpoint folder: {checkpointFolder} and output path: {output}")
        try:
            return (input.writeStream\
                .format('parquet')\
                .option('path', output)\
                .option('checkpointLocation', checkpointFolder)\
                .outputMode('append')\
                .start())
        except Exception as e:
            logger.error(f"{e}: Error starting stream writer with checkpoint folder: {checkpointFolder} and output path: {output}", exc_info=True)
            raise      

    logging.info("Streaming is being started...")
    try:
        tickerDf = read_kafka_topic(topic_name, schema)

        if tickerDf:
            streaming_query = streamWriter(tickerDf, checkpoint_folder, output_folder)
            streaming_query.awaitTermination()
            logging.info("data inserted into s3")
        else:
            logger.error("Ticker dataframe is None. Exiting application.")
    except Exception as e:
        logger.error("Error in streaming application", exc_info=True)

if __name__ == "__main__":
    main()