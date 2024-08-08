import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType,
                               StructField, StructType, TimestampType)

from config import configuration

# Configure logging
todays_date = str(datetime.today().date()).replace('-','_')
log_file = f'logs/consumer_{todays_date}.log'
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler(log_file),
                        logging.StreamHandler()
                    ])
logger = logging.getLogger(__name__)

def main():

    schema = StructType([
        StructField("t", StringType(), False),       # Exchangesegment_ExchangeInstrumentID(exchange segment enum with underscore along with instumentID of the particular subscribed instrument)
        StructField("o", DoubleType(), False),       # Open
        StructField("h", DoubleType(), False),         # High
        StructField("l", DoubleType(), False),         # Low
        StructField("c", DoubleType(), False),         # Close
        StructField("bt", TimestampType(), False),        # Bar Time
        StructField("bv", LongType(), False),      # Bar Volume
        StructField("pv", DoubleType(), False)    # Sum of Quantity in Price
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
                .option('kafka.bootstrap.servers', 'broker:29092') \
                .option('subscribe', topic) \
                .option('startingOffsets', 'earliest') \
                .load()

            logging.info("kafka dataframe created successfully")

        except Exception as e:
            logging.warning(f"kafka dataframe could not be created because: {e}")

        return spark_df


    def streamWriter(input, checkpointFolder, output):
        logger.info(f"Starting stream writer with checkpoint folder: {checkpointFolder} and output path: {output}")
        try:
            return (input.writeStream\
                .format('parquet')\
                    .option('checkpointLocation', checkpointFolder)\
                        .option('path', output)\
                            .outputMode('append')\
                                .start())
        except Exception as e:
            logger.error(f"Error starting stream writer with checkpoint folder: {checkpointFolder} and output path: {output}", exc_info=True)
            raise      

    logging.info("Streaming is being started...")
    try:
        tickerDf = read_kafka_topic('market_data_topic', schema).alias('market')
        tickerDf = tickerDf.selectExpr("CAST(value AS STRING)")\
        .select(from_json(col('value'), schema).alias('data'))\
        .select("data.*")\
        .withWatermark('timestamp', '5 minutes')

        streaming_query = streamWriter(tickerDf, 's3a://algo-iifl/checkpoints/candle_data', 's3a://algo-iifl/candle_data')
        streaming_query.awaitTermination()
    except Exception as e:
        logger.error("Error in streaming application", exc_info=True)

if __name__ == "__main__":
    main()