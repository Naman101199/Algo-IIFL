import pandas as pd

api_ohlc = pd.read_csv('/home/ec2-user/Algo-IIFL/data/2024_08_08/api_ohlc.csv')
s3_ohlc = pd.read_csv('/home/ec2-user/Algo-IIFL/data/s3_ohlc.csv')

print(api_ohlc.info())
print(s3_ohlc.info())

api_ohlc.columns  = [i + '_api' for i in api_ohlc.columns]
s3_ohlc.columns  = [i + '_s3' for i in s3_ohlc.columns]

print(api_ohlc.sort_values('Timestamp_api'))
print(s3_ohlc.sort_values('Timestamp_s3'))

compare_vol = api_ohlc. \
    merge(s3_ohlc, 
    left_on = ['exchangeInstrumentID_api', 'Timestamp_api'],
    right_on = ['exchangeInstrumentID_s3', 'Timestamp_s3'])

# Calculate the volume difference
compare_vol['Volume_Difference'] = compare_vol['Volume_api'] - compare_vol['Volume_s3']
print(compare_vol.count())

# compare_vol.to_csv('/home/ec2-user/Algo-IIFL/data/compare_vol.csv', index = False)