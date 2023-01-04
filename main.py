# =============================================================================
# IMPORTS
# =============================================================================
import os, boto3
from dydx3 import Client
from pprint import pprint
import dydx3.constants as dydx3_constants
import traceback
import datetime as dt
import pandas as pd, numpy as np
from dotenv import load_dotenv
import io

# =============================================================================
# CONSTANTS
# =============================================================================
client = Client(host="https://api.dydx.exchange")
MARKETS = ["BTC-USD"]
NOW = dt.datetime.now(dt.timezone.utc)
NOW_STR = str(NOW).split("+")[0]
TODAY_STR = str(NOW.replace(hour=0, minute=0, second=0, microsecond=0)).split(" ")[0]
CUR_MINUTE = NOW.replace(second=0, microsecond=0)
PREV_MINUTE = CUR_MINUTE - dt.timedelta(minutes=1)
CSV_HEADER = "timestamp,bid_price,ask_price,bid_size,ask_size"
CSV_HEADER = ["timestamp", "bid_price", "ask_price", "bid_size", "ask_size"]

# =============================================================================
# AWS CONFIG
# =============================================================================
load_dotenv()
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
S3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name="eu-west-2",
)

BUCKET_NAME = "dydx-orderbook"

# =============================================================================
# EXECUTION
# =============================================================================
def main():
    for market in MARKETS:
        bid_ask = get_bid_ask_from_dydx_for_market(market)
        push_bid_ask_to_s3(market, bid_ask)


# =============================================================================
# FETCH FROM EXCHANGE FOR market
# =============================================================================
def get_bid_ask_from_dydx_for_market(market):
    try:
        res = client.public.get_orderbook(market=market)
        orderbook = res.data
        return extract_top_of_orderbook(orderbook)
    except Exception:
        traceback.print_exc()
        return create_nan_bid_ask_dict()


# =============================================================================
# EXTRACT BEST BID AND ASKS
# =============================================================================
def extract_top_of_orderbook(orderbook):
    asks = process_orderbook_to_df(orderbook, "asks")
    bids = process_orderbook_to_df(orderbook, "bids")

    best_ask = asks.iloc[asks["price"].idxmin()]
    best_bid = bids.iloc[bids["price"].idxmax()]
    return {
        "timestamp": NOW_STR,
        "ask_price": best_ask["price"],
        "ask_size": best_ask["size"],
        "bid_price": best_bid["price"],
        "bid_size": best_bid["size"],
    }


# =============================================================================
# PUSH DATA TO S3, MAKE NEW FILE IF NEW DAY
# =============================================================================
def push_bid_ask_to_s3(market, bid_ask):
    filepath = f"{market}/{TODAY_STR}/{market}-{TODAY_STR}.csv"
    # row = convert_dict_to_csv_row(bid_ask)
    bid_ask = convert_bid_ask_to_df(bid_ask)

    if check_if_file_exists_in_s3(filepath):
        append_bid_ask_to_existing_csv(bid_ask, filepath)
    else:
        # create new CSV
        save_df_to_csv(bid_ask, filepath)


# =============================================================================
# APPEND TO EXISTING .CSV
# =============================================================================
def append_bid_ask_to_existing_csv(bid_ask, filepath):
    obj = S3.get_object(Bucket=BUCKET_NAME, Key=filepath)
    data = obj["Body"].read().decode("utf-8")
    df = pd.read_csv(io.StringIO(data))
    df = pd.concat([df, bid_ask], ignore_index=True)
    save_df_to_csv(df, filepath)


# =============================================================================
# Create a new csv and append data
# =============================================================================
def save_df_to_csv(df, filepath):
    df = df.set_index("timestamp")
    print(df)
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer)
    res = S3.put_object(Bucket=BUCKET_NAME, Key=filepath, Body=csv_buffer.getvalue())
    print(
        f"{filepath} saved with status code: {res['ResponseMetadata']['HTTPStatusCode']}"
    )


# =============================================================================
#
# HELPERS
#
# =============================================================================


# =============================================================================
# dYdX returns values as strings
# =============================================================================
def process_orderbook_to_df(orderbook, _type):
    df = pd.DataFrame(orderbook[_type])
    df["price"] = pd.to_numeric(df["price"])
    df["size"] = pd.to_numeric(df["size"])
    return df


# =============================================================================
# If exchange doesn't return proper data, create nan dictionary
# =============================================================================
def create_nan_bid_ask_dict() -> dict:
    return {
        "ask_price": np.nan,
        "ask_size": np.nan,
        "bid_price": np.nan,
        "bid_size": np.nan,
    }


# =============================================================================
# CHECK IF FILE EXISTS
# =============================================================================
def check_if_file_exists_in_s3(filepath):
    try:
        S3.head_object(Bucket=BUCKET_NAME, Key=filepath)
        return True
    except S3.exceptions.ClientError as e:
        if e.response["Error"]["Message"] == "Not Found":
            return False
        raise Exception(e)


# =============================================================================
# CONVERT THE BID ASK TO A CSV ROW
# =============================================================================
def convert_dict_to_csv_row(d):
    return f"{d['timestamp']},{d['bid_price']},{d['ask_price']},{d['bid_size']},{d['ask_size']}"


# =============================================================================
# CONVERT THE BID ASK TO A DATAFRAME
# =============================================================================
def convert_bid_ask_to_df(bid_ask):
    df = pd.DataFrame([bid_ask])
    df = df[CSV_HEADER]
    return df


if __name__ == "__main__":
    main()
