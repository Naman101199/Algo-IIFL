import sys
import os
from config import configuration
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from Connect import XTSConnect
from datetime import datetime
from config import configuration
# import pandas as pd

API_KEY = configuration['API_KEY']
API_SECRET = configuration['API_SECRET']
source = "WEBAPI"

xt = XTSConnect(API_KEY, API_SECRET, source)
response = xt.marketdata_login()

exchange = [xt.EXCHANGE_MCXFO]
master = xt.get_master(exchange)
master = master['result']

master = [scrip.split('|') for scrip in master.split('\n')]
print([i for i in master if i[3] == 'GOLD' and i[6] == 'OPTFUT'])