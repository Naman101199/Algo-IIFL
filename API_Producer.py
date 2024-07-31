import logging
from Connect import XTSConnect
from datetime import datetime
from MarketDataSocketClient import MDSocket_io
from config import configuration
from confluent_kafka import Producer

# Configure logging to log to both console and file
todays_date = str(datetime.today().date()).replace('-','_')
log_file = f'logs/market_data_{todays_date}.log'
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

# Kafka configuration
kafka_config = {
    'bootstrap.servers': 'localhost:9092',  # Update with your Kafka broker address
    'client.id': 'market_data_producer'
}
topic_name = 'market_data_topic'

# Create a Kafka producer
producer = Producer(kafka_config)

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

# Callback for connection
def on_connect():
    """Connect from the socket."""
    logger.info('Market Data Socket connected successfully!')

    # Subscribe to instruments
    response = xt.send_subscription(Instruments, 1501)
    logger.info("Subscription response: %s", response)

# Callback on receiving message
def on_message(data):
    logger.info('I received a message!')
    produce_to_kafka(topic_name, str(data))

# Callback for message code 1502 FULL
def on_message1501_json_partial(data):
    logger.info('I received a 1501 Touchline message!')
    produce_to_kafka(topic_name, str(data))

# Callback for disconnection
def on_disconnect():
    logger.warning('Market Data Socket disconnected!')

# Callback for error
def on_error(data):
    """Error from the socket."""
    logger.error('Market Data Error: %s', data)

# Assign the callbacks
soc.on_connect = on_connect
soc.on_message = on_message
soc.on_message1501_json_partial = on_message1501_json_partial
soc.on_disconnect = on_disconnect
soc.on_error = on_error

# Event listener
el = soc.get_emitter()
el.on('connect', on_connect)
el.on('1501-json-partial', on_message1501_json_partial)

# Infinite loop on the main thread. Nothing after this will run.
# You have to use the pre-defined callbacks to manage subscriptions.
soc.connect()