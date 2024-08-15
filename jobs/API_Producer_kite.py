import logging
from kiteconnect import KiteTicker, KiteConnect
from datetime import datetime
from confluent_kafka import Producer
import json
import time
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.config import configuration

# Set up the logging configuration
todays_date = str(datetime.today().date()).replace('-', '_')
log_file = f'/home/ec2-user/Algo-IIFL/logs/producer_kite_mcx_{todays_date}.log'
logging.basicConfig(
    level=logging.DEBUG,  # Changed to DEBUG
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

tokens = [8960002, 8982786]
topic_name= 'kite'
# [109831431,110148615]

PUBLIC_IP = configuration.get("PUBLIC_IP")
KITE_API_KEY = configuration.get("KITE_API_KEY")
KITE_ACCESS_KEY = configuration.get("KITE_ACCESS_KEY")

# Function to delivery callback
def delivery_report(err, msg):
    if err is not None:
        logger.error(f'Message delivery failed: {err}')
    else:
        logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')

# Function to produce messages to Kafka
def produce_to_kafka(topic, value):
    producer.produce(topic, value=value, callback=delivery_report)
    producer.flush()

# Function to append data to local file
def append_to_local_file(file_name, data):
    with open(file_name, 'a') as file:
        file.write(json.dumps(data) + '\n')

# Kafka configuration
kafka_config = {
    'bootstrap.servers': f'{PUBLIC_IP}:9092',  # Update with your Kafka broker address
    'client.id': 'market_data_producer'
}

# Create a Kafka producer
producer = Producer(kafka_config)

# Initialize KiteTicker
kws = KiteTicker(KITE_API_KEY, KITE_ACCESS_KEY)

# Callback on receiving message
def on_ticks(ws,ticks,topic_name = topic_name):
    # Check if the callback is getting called
    logger.debug("Entered on_ticks callback")
    if ticks:
        ticks = ticks[0]
        if 'depth' in ticks.keys():
            del ticks['ohlc']
            del ticks['depth']
            del ticks['tradable']
            ticks['last_trade_time'] = str(ticks['last_trade_time'])
            ticks['exchange_timestamp'] = str(ticks['exchange_timestamp'])        
        logger.info("Received ticks: {}".format(ticks))
        # Produce ticks to Kafka
        produce_to_kafka(topic_name, str(ticks))
    else:
        logger.debug("No ticks received.")

def on_connect(ws, response):
    # Callback on successful connect.
    logger.info(f'Market Data Socket connected successfully! - {response}')
    logger.info(f'Subscribed to tokens: {tokens}')
    ws.subscribe(tokens)

    # Set Instruments to tick in `QUOTE` mode.
    ws.set_mode(ws.MODE_FULL, tokens)

# Callback when current connection is closed.
def on_close(ws, code, reason):
    logger.info("Connection closed: {code} - {reason}".format(code=code, reason=reason))


# Callback when connection closed with error.
def on_error(ws, code, reason):
    logger.info("Connection error: {code} - {reason}".format(code=code, reason=reason))


# Callback when reconnect is on progress
def on_reconnect(ws, attempts_count):
    logger.info("Reconnecting: {}".format(attempts_count))


# Callback when all reconnect failed (exhausted max retries)
def on_noreconnect(ws):
    logger.info("Reconnect failed.")


# Assign the callbacks.
kws.on_ticks = on_ticks
kws.on_close = on_close
kws.on_error = on_error
kws.on_connect = on_connect
kws.on_reconnect = on_reconnect
kws.on_noreconnect = on_noreconnect


# Infinite loop on the main thread. Nothing after this will run.
# You have to use the pre-defined callbacks to manage subscriptions.
kws.connect()