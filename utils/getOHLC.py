import pandas as pd

def ohlcDataFrame(xt, exchangeSegment, exchangeInstrumentIDs: list, startTime: str, endTime: str, compressionValue: int):

    full_df = pd.DataFrame()

    for exchangeInstrumentID in exchangeInstrumentIDs:

        ohlc = xt.get_ohlc(
            exchangeSegment=exchangeSegment,
            exchangeInstrumentID=exchangeInstrumentID,
            startTime=startTime,
            endTime=endTime,
            compressionValue=compressionValue)

        # Remove the trailing delimiters
        cleaned_data_string = ohlc['result']['dataReponse'].rstrip(',')

        # Splitting the string into rows and then columns
        rows = [row.split('|') for row in cleaned_data_string.split(',') if row]

        # Remove any empty strings that could cause issues
        rows = [[item for item in row if item] for row in rows]

        # Defining column names
        columns = ["Timestamp", "Open", "High", "Low", "Close", "Volume", "OI"]

        # Removing the extra empty element at the end of each row
        rows = [row[:7] for row in rows]

        # Creating DataFrame
        df = pd.DataFrame(rows, columns=columns)
        df['Timestamp'] = pd.to_datetime(df['Timestamp'], unit='s').dt.strftime('%H:%M')
        df['exchangeSegment'] = exchangeSegment
        df['exchangeInstrumentID'] = int(exchangeInstrumentID)

        full_df = pd.concat([full_df, df], axis = 0)

    return full_df