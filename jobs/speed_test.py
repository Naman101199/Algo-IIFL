import speedtest
import time
import pandas as pd
import boto3
from datetime import datetime
from io import StringIO
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.config import configuration

# Initialize the speedtest object
st = speedtest.Speedtest()
st.get_best_server()

# Initialize boto3 client for S3
s3_client = boto3.client(
    's3',
    aws_access_key_id=configuration['AWS_ACCESS_KEY'],
    aws_secret_access_key=configuration['AWS_SECRET_KEY']
)

# S3 bucket and file details
bucket_name = 'algo-kite'
file_name = 'ping_data.csv'

def upload_to_s3(dataframe, bucket, file_name):
    """Upload the DataFrame to S3 as a CSV file."""
    csv_buffer = StringIO()
    dataframe.to_csv(csv_buffer, index=False)
    s3_client.put_object(Bucket=bucket, Key=file_name, Body=csv_buffer.getvalue())

# Initialize an empty DataFrame
df = pd.DataFrame(columns=['timestamp', 'latency', 'sponsor', 'distance'])

try:
    while True:
        server = st.results.server
        latency = server.get('latency')
        sponsor = server.get('sponsor')
        distance = round(server.get('d'), 2)

        timestamp = str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        # Create a new DataFrame with the current row
        new_row = pd.DataFrame({'timestamp': [timestamp], 'latency': [latency], 'sponsor': [sponsor], 'distance': [distance]})

        # Concatenate the new data to the DataFrame
        df = pd.concat([df, new_row], ignore_index=True)

        # Upload to S3
        upload_to_s3(df, bucket_name, file_name)

        # Print the current ping
        # print(f"Timestamp: {timestamp}, latency: {latency} ms, sponsor: {sponsor}, distance: {distance} km")

        # Wait for 1 second
        time.sleep(1)

except KeyboardInterrupt:
    print("Process interrupted by user.")
