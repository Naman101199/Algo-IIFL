# from XTConnect import XTSConnect
from Connect import XTSConnect
from datetime import datetime
from MarketDataSocketClient import MDSocket_io
from config import configuration

API_KEY = configuration['API_KEY']
API_SECRET = configuration['API_SECRET']
source = "WEBAPI"

xt = XTSConnect(API_KEY, API_SECRET, source)
response = xt.marketdata_login()

# Store the token and userid
set_marketDataToken = response['result']['token']
set_muserID = response['result']['userID']


# # Connecting to Marketdata socket
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
    print('Market Data Socket connected successfully!')

    # # Subscribe to instruments
    print('Sending subscription request for Instruments - \n' + str(Instruments))
    response = xt.send_subscription(Instruments, 1501)
    print('Sent Subscription request!')
    print("Subscription response: ", response)

# Callback on receiving message
def on_message(data):
    print('I received a message!')

# Callback for message code 1502 FULL
def on_message1502_json_full(data):
    print('I received a 1502 Market depth message!' + data)

# Callback for disconnection
def on_disconnect():
    print('Market Data Socket disconnected!')


# Callback for error
def on_error(data):
    """Error from the socket."""
    print('Market Data Error', data)


# Assign the callbacks.
soc.on_connect = on_connect
soc.on_message = on_message
soc.on_message1502_json_full = on_message1502_json_full
soc.on_disconnect = on_disconnect
soc.on_error = on_error


# Event listener
el = soc.get_emitter()
el.on('connect', on_connect)
el.on('1502-json-full', on_message1502_json_full)

# Infinite loop on the main thread. Nothing after this will run.
# You have to use the pre-defined callbacks to manage subscriptions.
soc.connect()
