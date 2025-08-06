import yfinance as yf
from pycoingecko import CoinGeckoAPI
from datetime import datetime, timedelta
import pytz

def get_percentage_change(current, previous):
    try:
        return round(((current - previous) / previous) * 100, 1)
    except ZeroDivisionError:
        return 0.0

def get_yf_change(ticker, date_current, date_1d, date_1m, date_ytd):
    try:
        data = yf.download(ticker, start=date_ytd.strftime('%Y-%m-%d'), end=(date_current + timedelta(days=1)).strftime('%Y-%m-%d'), progress=False)
        data = data["Adj Close"].dropna()
        today = data.iloc[-1]
        one_day_ago = data.loc[data.index.get_loc(date_1d, method='nearest')]
        one_month_ago = data.loc[data.index.get_loc(date_1m, method='nearest')]
        ytd = data.loc[data.index.get_loc(date_ytd, method='nearest')]

        return {
            "Today": float(today),
            "Change": get_percentage_change(today, one_day_ago),
            "Monthly": get_percentage_change(today, one_month_ago),
            "YTD": get_percentage_change(today, ytd)
        }
    except Exception as e:
        print(f"❌ Error fetching {ticker} from Yahoo: {str(e)}")
        return None

def get_closest_price(cg, target_date, window):
    history = cg.get_coin_market_chart_range_by_id(
        "bitcoin", "zar",
        int((target_date - window).timestamp()),
        int((target_date + window).timestamp())
    )
    prices = history.get("prices", [])
    closest = min(prices, key=lambda x: abs(x[0] / 1000 - target_date.timestamp()))
    return closest[1] if closest else None

def fetch_market_data():
    try:
        now = datetime.now(pytz.timezone("Africa/Johannesburg"))
        date_current = now
        date_1d = now - timedelta(days=1)
        date_1m = now - timedelta(days=30)
        date_ytd = datetime(now.year, 1, 1, tzinfo=now.tzinfo)

        # Yahoo Finance tickers
        tickers = {
            "JSEALSHARE": "^J203.JO",
            "USDZAR": "USDZAR=X",
            "EURZAR": "EURZAR=X",
            "GBPZAR": "GBPZAR=X",
            "BRENT": "BZ=F",
            "SP500": "^GSPC"
        }

        results = {}

        for key, ticker in tickers.items():
            data = get_yf_change(ticker, date_current, date_1d, date_1m, date_ytd)
            if data:
                results[key] = data
            else:
                raise ValueError(f"Failed to fetch valid data for {key}")

        # CoinGecko: Bitcoin
        cg = CoinGeckoAPI()
        btc_today = cg.get_price(ids="bitcoin", vs_currencies="zar")["bitcoin"]["zar"]
        btc_1d = get_closest_price(cg, date_1d, timedelta(hours=6))
        btc_1m = get_closest_price(cg, date_1m, timedelta(hours=6))
        btc_ytd = get_closest_price(cg, date_ytd, timedelta(hours=6))

        results["BITCOINZAR"] = {
            "Today": btc_today,
            "Change": get_percentage_change(btc_today, btc_1d),
            "Monthly": get_percentage_change(btc_today, btc_1m),
            "YTD": get_percentage_change(btc_today, btc_ytd)
        }

        # CoinGecko: Gold fallback
        try:
            gold_data = cg.get_price(ids="gold", vs_currencies="zar")["gold"]["zar"]
            gold_1d = get_closest_price(cg, date_1d, timedelta(hours=6))
            gold_1m = get_closest_price(cg, date_1m, timedelta(hours=6))
            gold_ytd = get_closest_price(cg, date_ytd, timedelta(hours=6))
            results["GOLD"] = {
                "Today": gold_data,
                "Change": get_percentage_change(gold_data, gold_1d),
                "Monthly": get_percentage_change(gold_data, gold_1m),
                "YTD": get_percentage_change(gold_data, gold_ytd)
            }
        except Exception as fallback_e:
            print("⚠️ CoinGecko fallback for Gold failed:", fallback_e)
            gold_yf = get_yf_change("GC=F", date_current, date_1d, date_1m, date_ytd)
            if gold_yf:
                results["GOLD"] = gold_yf
            else:
                raise RuntimeError("Gold data could not be fetched from either source.")

        results["timestamp"] = now.strftime("%Y-%m-%d %H:%M")
        return results

    except Exception as e:
        print(f"❌ Critical error in fetch_market_data: {str(e)}")
        return None
