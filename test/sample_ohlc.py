import sys
import os
from config import configuration
from getOHLC import ohlcDataFrame
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from Connect import XTSConnect

API_KEY = configuration['API_KEY']
API_SECRET = configuration['API_SECRET']
source = "WEBAPI"

xt = XTSConnect(API_KEY, API_SECRET, source)
response = xt.marketdata_login()

exchangeSegment = xt.EXCHANGE_NSEFO
exchangeInstrumentIDs = ['35089', '35415']
startTime = 'Aug 7 2024 090000'
endTime = 'Aug 7 2024 153000'
compressionValue = 60

df = ohlcDataFrame(xt, exchangeSegment, exchangeInstrumentIDs, startTime, endTime, compressionValue)
print(df)