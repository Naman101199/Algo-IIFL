from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, LongType, TimestampType
from config import configuration
import logging
from datetime import datetime

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

    topic_name = 'message1512_json_full'
    checkpointFolder = f's3a://algo-iifl/checkpoints/tick_data/{topic_name}/{todays_date}'
    output = f's3a://algo-iifl/data/{topic_name}/{todays_date}'

    schema = StructType([
        StructField("MessageCode", IntegerType(), False),
        StructField("MessageVersion", IntegerType(), False),
        StructField("ApplicationType", IntegerType(), False),
        StructField("TokenID", IntegerType(), False),
        StructField("ExchangeSegment", IntegerType(), False),
        StructField("ExchangeInstrumentID", IntegerType(), False),
        StructField("BookType", IntegerType(), False),
        StructField("XMarketType", IntegerType(), False),
        StructField("LastTradedPrice", DoubleType(), False),
        StructField("LastTradedQunatity", IntegerType(), False),
        StructField("LastUpdateTime", LongType(), False)
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
            streaming_query = streamWriter(tickerDf, checkpointFolder, output)
            streaming_query.awaitTermination()
        else:
            logger.error("Ticker dataframe is None. Exiting application.")
    except Exception as e:
        logger.error("Error in streaming application", exc_info=True)

if __name__ == "__main__":
    main()