import sys
import os
from config import configuration
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from Connect import XTSConnect
from datetime import datetime
from config import configuration
import pandas as pd

API_KEY = configuration['API_KEY']
API_SECRET = configuration['API_SECRET']
source = "WEBAPI"

xt = XTSConnect(API_KEY, API_SECRET, source)
response = xt.marketdata_login()

exchange = [xt.EXCHANGE_NSEFO]
master = xt.get_master(exchange)
master = master['result']

# with open("../Algo-IIFL/FO.txt", "w") as master_file:
#     master_file.write(master)

# master_df = pd.read_csv("../Algo-IIFL/FO.txt", sep = "|", usecols=range(19), header=None, low_memory=True)

# Split the text into individual lines and remove any empty lines
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
df.to_csv('master.csv', header=False)