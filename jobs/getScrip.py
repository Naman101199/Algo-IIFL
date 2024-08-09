#!/home/ec2-user/myenv/bin/env python3

import sys
import os
from datetime import datetime
import pandas as pd
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import time
from utils.Connect import XTSConnect
from utils.config import configuration

API_KEY = configuration['API_KEY']
API_SECRET = configuration['API_SECRET']
source = "WEBAPI"

xt = XTSConnect(API_KEY, API_SECRET, source)
response = xt.marketdata_login()

exchange = [xt.EXCHANGE_NSEFO]
master = xt.get_master(exchange)
master = master['result']

lines = [line for line in master.strip().split("\n")]

# Split each line by the separator
data = [line.split("|") for line in lines]

columns = [
    "ExchangeSegment",
    "ExchangeInstrumentID",
    "InstrumentType",
    "Name",
    "Description",
    "Series",
    "NameWithSeries",
    "InstrumentID",
    "PriceBand.High",
    "PriceBand.Low",
    "FreezeQty",
    "TickSize",
    "LotSize",
    "Multiplier",
    "UnderlyingInstrumentId",
    "UnderlyingIndexName",
    "ContractExpiration",
    "StrikePrice",
    "OptionType",
    "DisplayName",
    "PriceNumerator",
    "PriceDenominator",
    "DetailedDescription"
]
df = pd.DataFrame(data, columns=columns)

# Function to shift columns for FUTSTK series
def shift_columns(row):
    if 'FUT' in row['Series']:
        row['DetailedDescription'] = row['PriceNumerator']
        row['PriceDenominator'] = row['DisplayName']
        row['PriceNumerator'] = row['OptionType']
        row['DisplayName'] = row['StrikePrice']
        row['StrikePrice'] = None
        row['OptionType'] = None
    return row

# Apply the function to each row in the dataframe
df = df.apply(shift_columns, axis=1)
print(df[df['ExchangeInstrumentID'].isin(['35089','35415'])])