class DydxHelper:
    def __init__(self, wallet, api_key, api_secret, passphrase, stark, token):

        self.client = DydxClient(wallet, api_key, api_secret, passphrase, stark)
        self.token = token
        self.token_formatter()

        # self.order_params = {token: self.get_order_params(token) for token in self.token}
        self.order_params = {self.token: self.get_order_params(self.token)}
        self.tick_adjust = (
            self.order_params[self.token]["price_rounder"] / 2
        )  # this will put you 1 tick above/below

    def get_order_params(self, token):

        # TODO - could add min order size here too... would help with crumbs later.

        params = self.client.get_token_info(token)
        tick_size = float(params["tickSize"])
        order_size = float(params["stepSize"])
        min_order = float(params["minOrderSize"])

        return {
            "price_rounder": tick_size,
            "order_rounder": order_size,
            "min_order_size": min_order,
        }

    def round_to_value(self, number, precision):

        """
        Rounds a number to desired precision - say you want to round to nearest half decimal (0.5, or 0.05, etc).

        Parameters
        ----------
        number : float
            number to be rounded.
        precision : float
            this is the desired rounder - as in if I wanted 1.05, I'd pass (number = 1.03, precision = 0.05).

        Returns
        -------
        rounded : float
            number rounded to desired accuracy, with adjustment for float artifacts.

        """

        rounded = round(number / precision) * precision
        return round(rounded, 6)

    def date_to_unix(self, date):

        """
        This turns a date into a unix-stamp. - Should move this to Swan package
        """

        date = pd.to_datetime(date)
        unix = time.mktime(date.timetuple())
        unix = int(round(unix, 0))

        return unix

    def token_formatter(self):

        # TODO - should maybe wrap this around each self.token call? Then in the hall monitor
        # could just pass the base token like: "BTC" and have each exchange format to it's own way?
        self.token = f"{self.token}-USD"
        print(f"DYDX tokens now formatted to: {self.token}")

    def perc_diff(self, value1, value2):

        """
        Returns the percent difference between the two values.
        """

        diff = abs(value1 - value2) / ((value1 + value2) / 2)
        return diff

    def tick_diff(self, bid, ask):

        ticks = round((ask - bid) / self.order_params[self.token]["price_rounder"], 3)
        return abs(ticks)

    def get_bid_ask(self):

        # timestamp = datetime.datetime.utcnow()
        # orderbook = self.public_client.public.get_orderbook(market=self.token).data

        orderbook = self.client.get_orderbook(self.token)

        bids = (
            pd.DataFrame(orderbook["bids"])
            .head(1)
            .rename(columns={"size": "bid_size", "price": "bid_price"})
        )
        asks = (
            pd.DataFrame(orderbook["asks"])
            .head(1)
            .rename(columns={"size": "ask_size", "price": "ask_price"})
        )
        # mid  = (bids["bid_price"].astype(float) + asks["ask_price"].astype(float)) / 2 # TODO - should upgrade to weighted mid?

        ladder = pd.concat([bids, asks], axis=1)
        ladder = ladder.astype(float)
        ladder = ladder.reindex(
            columns=["bid_price", "bid_size", "ask_price", "ask_size"]
        )
        # results["mid_price"] = mid
        # results.index = [timestamp]

        top_bid = ladder["bid_price"].head(1).values[0]
        top_ask = ladder["ask_price"].head(1).values[0]

        # diff = self.diff_check(top_bid, top_ask)
        diff = self.tick_diff(top_bid, top_ask)

        # TODO - want this to be like less than 3 ticks difference?
        if diff < 10.0:
            return ladder

        else:
            print(
                f"Bid ask was too far apart for {self.token} on DYDX at bid: {top_bid}, ask: {top_ask}."
            )
            # TODO - send alert.
            time.sleep(0.50)
            self.get_bid_ask()

        return ladder

    def get_mid_price(self):

        """
        Calculates mid price, and makes sure that the bid/ask aren't super far apart.
        """

        ladder = self.get_bid_ask()
        top_bid = ladder["bid_price"].head(1).values[0]
        top_ask = ladder["ask_price"].head(1).values[0]

        mid = (top_bid + top_ask) / 2
        # mid = self.price_rounder(mid) #TODO - round in the long/short part away from that direction.

        print(f"\nMid price from Dydx for {self.token}: {mid}")
        return mid
