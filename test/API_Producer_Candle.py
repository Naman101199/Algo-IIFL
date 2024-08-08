import sys
import os
from config import configuration
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
from Connect import XTSConnect
from datetime import datetime
from MarketDataSocketClient import MDSocket_io
from confluent_kafka import Producer
import json
import uuid

#Enviroment Variables for configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC_NAME = os.getenv('TOPIC_NAME', 'market_data_topic')

# Configure logging to log to both console and file
todays_date = str(datetime.today().date()).replace('-','_')
log_file = f'logs/producer_{todays_date}.log'
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler(log_file),
                        logging.StreamHandler()
                    ])
logger = logging.getLogger(__name__)

API_KEY = configuration['API_KEY']
API_SECRET = configuration['API_SECRET']
source = "WEBAPI"


# Function to delivery callback
def delivery_report(err, msg):
    if err is not None:
        logger.error(f'Message delivery failed: {err}')
    else:
        logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')

# def json_serializer(obj):
#     if isinstance(obj, uuid.UUID):
#         return str(obj)
#     raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')


# Function to produce messages to Kafka
def produce_to_kafka(producer, topic, value):

    producer.produce(topic, 
                        value=json.dumps(value).encode('utf-8'),
                        callback=delivery_report)
    producer.flush()

# Callback for connection
def on_connect():
    """Connect from the socket."""
    logger.info('Market Data Socket connected successfully!')

    # Subscribe to instruments
    response = xt.send_subscription(Instruments, 1501)
    # logger.info("Subscription got response")

# Callback on receiving message
def on_message(data):
    produce_to_kafka(producer, TOPIC_NAME, str(data))
    logger.info('I received a message and sent to producer!')

# Callback for message code 1502 FULL
def on_message1105_json_partial(data):
    produce_to_kafka(producer, TOPIC_NAME, str(data))
    logger.info('I received a 1505 Candle data message and sent to producer!')

# Callback for disconnection
def on_disconnect():
    logger.warning('Market Data Socket disconnected!')

# Callback for error
def on_error(data):
    """Error from the socket."""
    logger.error('Market Data Error: %s', data)


if __name__ == "__main__":
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print(f'Kafka error: {err}')
    }

    producer = Producer(producer_config)

    xt = XTSConnect(API_KEY, API_SECRET, source)
    response = xt.marketdata_login()

    # Store the token and userid
    set_marketDataToken = response['result']['token']
    set_muserID = response['result']['userID']

    # Connecting to Marketdata socket
    soc = MDSocket_io(set_marketDataToken, set_muserID)

    # Instruments for subscribing
    Instruments = [
        {'exchangeSegment': 1, 'exchangeInstrumentID': 2885},
        {'exchangeSegment': 1, 'exchangeInstrumentID': 26000},
        {'exchangeSegment': 2, 'exchangeInstrumentID': 51601}
    ]

    # Assign the callbacks
    soc.on_connect = on_connect
    # soc.on_message = on_message
    soc.on_message1105_json_partial = on_message1105_json_partial
    soc.on_disconnect = on_disconnect
    soc.on_error = on_error

    # Event listener
    el = soc.get_emitter()
    el.on('connect', on_connect)
    el.on('1105-json-partial', on_message1105_json_partial)

    # Infinite loop on the main thread. Nothing after this will run.
    # You have to use the pre-defined callbacks to manage subscriptions.
    soc.connect()