from dydx3 import Client
from pprint import pprint
import dydx3.constants as dydx3_constants

pairs = [
    "AAVE-USD"
    "ADA-USD"
    "ALGO-USD"
    "ATOM-USD"
    "AVAX-USD"
    "BCH-USD"
    "BTC-USD"
    "CELO-USD"
    "COMP-USD"
    "CRV-USD"
    "DOGE-USD"
    "DOT-USD"
    "ENJ-USD"
    "EOS-USD"
    "ETC-USD"
    "ETH-USD"
    "FIL-USD"
    "ICP-USD"
    "LINK-USD"
    "LTC-USD"
    "LUNA-USD"
    "MATIC-USD"
    "MKR-USD"
    "NEAR-USD"
    "1INCH-USD"
    "RUNE-USD"
    "SNX-USD"
    "SOL-USD"
    "SUSHI-USD"
    "TRX-USD"
    "UMA-USD"
    "UNI-USD"
    "XLM-USD"
    "XMR-USD"
    "XTZ-USD"
    "YFI-USD"
    "ZEC-USD"
    "ZRX-USD"
]

client = Client(host="https://api.dydx.exchange")

candles = client.public.get_candles(market="BTC-USD", resolution="1MIN", limit=5)
pprint(candles.data)
# markets = client.public.get_markets()
# tokens = [t for t in markets.data["markets"].keys()]
# pprint(tokens)


############ FOR MOONSWAN ############

# Another question: For OHCLV, dYdX updates candles after they're done. As you can see below, 3 dictionaries with slightly different values, **all fetched after minute was complete**:

# `{'baseTokenVolume': '20.8478',
#  'close': '16643',
#  'high': '16647',
#  'low': '16640',
#  'market': 'BTC-USD',
#  'open': '16641',
#  'resolution': '1MIN',
#  'startedAt': '2023-01-03T16:15:00.000Z',
#  'startingOpenInterest': '4793.1890',
#  'trades': '55',
#  'updatedAt': '2023-01-03T16:15:57.481Z',
#  'usdVolume': '346993.8723'}`

# `{'baseTokenVolume': '20.8358',
#  'close': '16643',
#  'high': '16647',
#  'low': '16640',
#  'market': 'BTC-USD',
#  'open': '16641',
#  'resolution': '1MIN',
#  'startedAt': '2023-01-03T16:15:00.000Z',
#  'startingOpenInterest': '4793.1890',
#  'trades': '52',
#  'updatedAt': '2023-01-03T16:15:41.836Z',
#  'usdVolume': '346794.1463'}`

# `{'baseTokenVolume': '15.5288',
#  'close': '16647',
#  'high': '16647',
#  'low': '16640',
#  'market': 'BTC-USD',
#  'open': '16641',
#  'resolution': '1MIN',
#  'startedAt': '2023-01-03T16:15:00.000Z',
#  'startingOpenInterest': '4793.1890',
#  'trades': '42',
#  'updatedAt': '2023-01-03T16:15:22.286Z',
#  'usdVolume': '258454.6653'}`
