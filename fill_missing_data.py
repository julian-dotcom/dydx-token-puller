# =============================================================================
# IMPORTS
# =============================================================================
import sys, os, pytz, time, traceback
import boto3
import io
import datetime as dt
from dotenv import load_dotenv
import pandas as pd
from pprint import pprint

load_dotenv()
# =============================================================================
# CONFIG
# =============================================================================
BUCKET_NAME = "dydx-orderbook"
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
S3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name="eu-west-2",
)


binance = pd.read_csv("sol-data.csv")
filepath = "SOL-USD/SOL-USD_2023-01-19.csv"
csv = S3.get_object(Bucket=BUCKET_NAME, Key=filepath)
fucked = csv["Body"].read().decode("utf-8")
fucked = pd.read_csv(io.StringIO(fucked))
fucked = fucked.iloc[892:].reset_index(drop=True)
result = pd.concat([binance, fucked], axis=0, ignore_index=True)
result = result.set_index("timestamp")

csv_buffer = io.StringIO()
result.to_csv(csv_buffer)
print(result)
print(filepath)
res = S3.put_object(Bucket=BUCKET_NAME, Key=filepath, Body=csv_buffer.getvalue())
# print(
#     f"{filepath} saved with status code: {res['ResponseMetadata']['HTTPStatusCode']}"
# )
