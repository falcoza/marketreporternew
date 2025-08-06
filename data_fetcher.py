from datetime import datetime, timedelta
import yfinance as yf
from pycoingecko import CoinGeckoAPI
import pytz

# Helper function to extract closing prices with fallback
def get_price_column(data):
    if "Adj Close" in data.columns:
        return data["Adj Close"]
    elif "Close" in data.columns:
        return data["Close"]
    else:
        raise KeyError("Neither 'Adj Close' nor 'Close' found in data")

def fetch_market_data():
    try:
        tz = pytz.timezone("Africa/Johannesburg")
        date_current = datetime.now(tz)
        date_ytd = datetime(date_current.year, 1, 1, tzinfo=tz)
        date_1mo = date_current - timedelta(days=30)
        date_1d = date_current - timedelta(days=1)

        tickers = {
            "JSEALSHARE": "^J203.JO",
            "USDZAR": "USDZAR=X",
            "EURZAR": "EURZAR=X",
            "GBPZAR": "GBPZAR=X",
            "BRENT": "BZ=F",
            "SP500": "^GSPC"
        }

        data_dict = {}
        for key, ticker in tickers.items():
            try:
                data = yf.download(
                    ticker,
                    start=date_ytd.strftime('%Y-%m-%d'),
                    end=(date_current + timedelta(days=1)).strftime('%Y-%m-%d'),
                    progress=False
                )

                price_col = get_price_column(data)

                today = price_col[-1]
                prev = price_col[price_col.index < price_col.index[-1]][-1]
                month = price_col[price_col.index >= date_1mo.strftime('%Y-%m-%d')][0]
                ytd = price_col[0]

                data_dict[key] = {
                    "Today": float(today),
                    "Change": ((today - prev) / prev) * 100,
                    "Monthly": ((today - month) / month) * 100,
                    "YTD": ((today - ytd) / ytd) * 100,
                }
            except Exception as e:
                print(f"❌ Error fetching {ticker} from Yahoo: {e}")
                raise ValueError(f"Failed to fetch valid data for {key}")

        # CoinGecko for crypto + gold
        cg = CoinGeckoAPI()

        # Bitcoin price in ZAR
        btc_price = cg.get_price(ids="bitcoin", vs_currencies="zar")["bitcoin"]["zar"]
        btc_history = cg.get_coin_market_chart_range_by_id(
            id="bitcoin",
            vs_currency="zar",
            from_timestamp=int(date_ytd.timestamp()),
            to_timestamp=int(date_current.timestamp())
        )["prices"]

        btc_prices = [p[1] for p in btc_history]
        btc_ytd = btc_prices[0]
        btc_1mo = btc_prices[max(0, len(btc_prices) - 30)]
        btc_1d = btc_prices[-2]
        btc_today = btc_prices[-1]

        data_dict["BITCOINZAR"] = {
            "Today": btc_today,
            "Change": ((btc_today - btc_1d) / btc_1d) * 100,
            "Monthly": ((btc_today - btc_1mo) / btc_1mo) * 100,
            "YTD": ((btc_today - btc_ytd) / btc_ytd) * 100,
        }

        # Gold price from CoinGecko (only today's value)
        gold_price = cg.get_price(ids="gold", vs_currencies="zar")["gold"]["zar"]
        data_dict["GOLD"] = {
            "Today": gold_price,
            "Change": 0.0,
            "Monthly": 0.0,
            "YTD": 0.0,
        }

        data_dict["timestamp"] = date_current.strftime('%Y-%m-%d')
        return data_dict

    except Exception as e:
        print(f"❌ Critical error in fetch_market_data: {str(e)}")
        return None
