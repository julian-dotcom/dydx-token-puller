# =============================================================================
# IMPORTS
# =============================================================================
import os, boto3, time
from pprint import pprint
import traceback
import datetime as dt
import pandas as pd, numpy as np
from dotenv import load_dotenv
import io
import requests
import concurrent.futures

# =============================================================================
# CONSTANTS
# =============================================================================
DYDX_URL = "https://api.dydx.exchange/v3"
MARKETS = [
    "1INCH-USD",
    "AAVE-USD",
    "ADA-USD",
    "ALGO-USD",
    "ATOM-USD",
    "AVAX-USD",
    "BTC-USD",
    "BCH-USD",
    "CELO-USD",
    "ETC-USD",
    "COMP-USD",
    "ICP-USD",
    "CRV-USD",
    "DOGE-USD",
    "DOT-USD",
    "ENJ-USD",
    "EOS-USD",
    "ETH-USD",
    "FIL-USD",
    "LINK-USD",
    "LTC-USD",
    "MATIC-USD",
    "MKR-USD",
    "NEAR-USD",
    "RUNE-USD",
    "SNX-USD",
    "SOL-USD",
    "SUSHI-USD",
    "TRX-USD",
    "UMA-USD",
    "UNI-USD",
    "XLM-USD",
    "XMR-USD",
    "XTZ-USD",
    "YFI-USD",
    "ZEC-USD",
    "ZRX-USD",
]


CSV_HEADER = [
    "timestamp",
    "bid_price",
    "ask_price",
    "mid",
    "bid_size",
    "ask_size",
]

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
    market_params = get_detailed_market_params()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        result = executor.map(get_bid_ask_from_dydx_for_market, MARKETS)

    now_s, today = create_relevant_date_strings()
    for bid_ask in result:
        bid_ask = check_if_bid_ask_proper(market_params, bid_ask)
        bid_ask = add_mid_to_bid_ask(bid_ask)
        push_bid_ask_to_s3(bid_ask, now_s, today)


# =============================================================================
# GET DETAILS LIKE TICK_SIZE, STEP_SIZE, ETC.
# =============================================================================
def get_detailed_market_params():
    res = requests.get(f"{DYDX_URL}/markets")
    res = res.json()["markets"]
    market_params = {}

    for market, data in res.items():
        if market not in MARKETS:
            continue
        tick_size = float(data["tickSize"])
        order_size = float(data["stepSize"])
        min_order = float(data["minOrderSize"])

        market_params[market] = {
            "price_rounder": tick_size,
            "order_size": order_size,
            "min_order": min_order,
        }
    return market_params


# =============================================================================
# FETCH FROM EXCHANGE FOR market
# =============================================================================
def get_bid_ask_from_dydx_for_market(market):
    counter = 0
    while counter < 10:
        try:
            res = requests.get(f"{DYDX_URL}/orderbook/{market}")
            orderbook = res.json()
            return extract_top_of_orderbook(market, orderbook)
        except Exception:
            print(f"Failed fetching data for market: {market}")
            traceback.print_exc()
            time.sleep(0.5)
            counter += 1
    return create_nan_bid_ask_dict(market)


# =============================================================================
# EXTRACT BEST BID AND ASKS
# =============================================================================
def extract_top_of_orderbook(market, orderbook):
    asks = process_orderbook_to_df(orderbook, "asks")
    bids = process_orderbook_to_df(orderbook, "bids")

    best_ask = asks.iloc[asks["price"].idxmin()]
    best_bid = bids.iloc[bids["price"].idxmax()]
    return {
        "market": market,
        "ask_price": best_ask["price"],
        "ask_size": best_ask["size"],
        "bid_price": best_bid["price"],
        "bid_size": best_bid["size"],
    }


# =============================================================================
# ENSURE THAT BID - ASK DIFF ISN'T TOO BIG
# =============================================================================
def check_if_bid_ask_proper(market_params, bid_ask):
    market = bid_ask["market"]
    params = market_params[market]
    diff = determine_tick_diff(params, bid_ask)
    if diff < 10:
        return bid_ask
    else:
        return create_nan_bid_ask_dict(market)


# =============================================================================
# CALC MID BETWEEN BID AND ASK
# =============================================================================
def add_mid_to_bid_ask(bid_ask):
    mid = (bid_ask["bid_price"] + bid_ask["ask_price"]) / 2
    bid_ask["mid"] = round(mid, 6)
    return bid_ask


# =============================================================================
# PUSH DATA TO S3, MAKE NEW FILE IF NEW DAY
# =============================================================================
def push_bid_ask_to_s3(bid_ask, now_s, today):
    market = bid_ask.pop("market")
    bid_ask["timestamp"] = now_s
    filepath = f"{market}/{market}_{today}.csv"
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
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer)
    print("Not saving for testing")
    print(df)
    # res = S3.put_object(Bucket=BUCKET_NAME, Key=filepath, Body=csv_buffer.getvalue())
    # print(
    #     f"{filepath} saved with status code: {res['ResponseMetadata']['HTTPStatusCode']}"
    # )


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
# Determine tick difference between bid, ask, to make sure orderbook is proper
# =============================================================================
def determine_tick_diff(params, bid_ask):
    ask, bid = bid_ask["ask_price"], bid_ask["bid_price"]
    ticks = round((ask - bid) / params["price_rounder"], 3)
    return abs(ticks)


# =============================================================================
# If exchange doesn't return proper data, create nan dictionary
# =============================================================================
def create_nan_bid_ask_dict(market) -> dict:
    return {
        "market": market,
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


# =============================================================================
# CREATE THE RELEVANT DATE STRINGS
# =============================================================================
def create_relevant_date_strings():
    now = dt.datetime.now(dt.timezone.utc)
    now_s = str(now).split("+")[0]
    today = str(now.replace(hour=0, minute=0, second=0, microsecond=0)).split(" ")[0]
    return now_s, today


if __name__ == "__main__":
    main()
