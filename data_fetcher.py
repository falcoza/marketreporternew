import yfinance as yf
from pycoingecko import CoinGeckoAPI
from datetime import datetime, timedelta, timezone
import pytz

cg = CoinGeckoAPI()
tz = pytz.timezone("Africa/Johannesburg")

def get_price_change(current, previous):
    return round(((current - previous) / previous) * 100, 1)

def fetch_from_yahoo(ticker, date_1d, date_1m, date_ytd, date_current):
    try:
        data = yf.download(
            ticker,
            start=date_ytd.strftime('%Y-%m-%d'),
            end=(date_current + timedelta(days=1)).strftime('%Y-%m-%d'),
            progress=False
        )

        if 'Adj Close' not in data.columns:
            raise KeyError(f"Adj Close not found for {ticker}")

        series = data['Adj Close']

        price_today = series.loc[series.index <= date_current][-1]
        price_1d = series.loc[series.index <= date_1d][-1]
        price_1m = series.loc[series.index <= date_1m][-1]
        price_ytd = series.loc[series.index <= date_ytd][-1]

        return {
            "Today": round(price_today, 2),
            "Change": get_price_change(price_today, price_1d),
            "Monthly": get_price_change(price_today, price_1m),
            "YTD": get_price_change(price_today, price_ytd),
        }

    except Exception as e:
        print(f"❌ Error fetching {ticker} from Yahoo: {e}")
        return None

def fetch_from_coingecko(id, vs_currency, target_date):
    try:
        window = timedelta(hours=12)
        history = cg.get_coin_market_chart_range_by_id(
            id, vs_currency,
            int((target_date - window).timestamp()),
            int((target_date + window).timestamp())
        )
        prices = history.get("prices", [])
        closest = min(prices, key=lambda x: abs(x[0]/1000 - target_date.timestamp()))
        return closest[1]
    except Exception as e:
        print(f"❌ Error fetching {id} from CoinGecko: {e}")
        return None

def fetch_bitcoin_zar(date_1d, date_1m, date_ytd, date_current):
    try:
        today_price = cg.get_price(ids="bitcoin", vs_currencies="zar")["bitcoin"]["zar"]
        price_1d = fetch_from_coingecko("bitcoin", "zar", date_1d)
        price_1m = fetch_from_coingecko("bitcoin", "zar", date_1m)
        price_ytd = fetch_from_coingecko("bitcoin", "zar", date_ytd)

        return {
            "Today": round(today_price, 0),
            "Change": get_price_change(today_price, price_1d),
            "Monthly": get_price_change(today_price, price_1m),
            "YTD": get_price_change(today_price, price_ytd)
        }
    except Exception as e:
        raise RuntimeError(f"Bitcoin ZAR fetch failed: {e}")

def fetch_gold_zar(date_1d, date_1m, date_ytd, date_current):
    try:
        gold_today = fetch_from_coingecko("gold", "zar", date_current)
        gold_1d = fetch_from_coingecko("gold", "zar", date_1d)
        gold_1m = fetch_from_coingecko("gold", "zar", date_1m)
        gold_ytd = fetch_from_coingecko("gold", "zar", date_ytd)

        return {
            "Today": round(gold_today, 0),
            "Change": get_price_change(gold_today, gold_1d),
            "Monthly": get_price_change(gold_today, gold_1m),
            "YTD": get_price_change(gold_today, gold_ytd)
        }
    except Exception as e:
        raise RuntimeError(f"Gold ZAR fetch failed: {e}")

def fetch_market_data():
    try:
        now = datetime.now(tz)
        date_current = now.replace(hour=0, minute=0, second=0, microsecond=0)
        date_1d = date_current - timedelta(days=1)
        date_1m = date_current - timedelta(days=30)
        date_ytd = datetime(date_current.year, 1, 1, tzinfo=tz)

        result = {}

        tickers = {
            "JSEALSHARE": "^J203.JO",
            "USDZAR": "USDZAR=X",
            "EURZAR": "EURZAR=X",
            "GBPZAR": "GBPZAR=X",
            "BRENT": "BZ=F",
            "SP500": "^GSPC"
        }

        for key, ticker in tickers.items():
            data = fetch_from_yahoo(ticker, date_1d, date_1m, date_ytd, date_current)
            if not data:
                raise RuntimeError(f"Failed to fetch valid data for {key}")
            result[key] = data

        result["BITCOINZAR"] = fetch_bitcoin_zar(date_1d, date_1m, date_ytd, date_current)
        result["GOLD"] = fetch_gold_zar(date_1d, date_1m, date_ytd, date_current)

        result["timestamp"] = now.strftime("%Y-%m-%d %H:%M")
        return result

    except Exception as e:
        print(f"❌ Critical error in fetch_market_data: {e}")
        return None
