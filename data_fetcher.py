import yfinance as yf
from pycoingecko import CoinGeckoAPI
from datetime import datetime, timezone, timedelta
import pytz
from typing import Optional, Dict, Any

def calculate_percentage(old: Optional[float], new: Optional[float]) -> float:
    if None in (old, new) or old is None or old == 0 or old < 10:
        return 0.0
    try:
        return ((new - old) / old) * 100
    except (TypeError, ZeroDivisionError):
        return 0.0

def fetch_historical(ticker: str, days: int) -> Optional[float]:
    try:
        buffer_days = max(5, days // 5)
        stock = yf.Ticker(ticker)
        data = stock.history(period=f"{days + buffer_days}d", interval="1d")
        if not data.empty and len(data) >= days + 1:
            price = data['Close'].iloc[-days-1]
            if price is None or price < 10:
                return None
            return price
        return None
    except Exception as e:
        print(f"⚠️ Historical data error for {ticker}: {str(e)}")
        return None

def get_ytd_reference_price(ticker: str) -> Optional[float]:
    try:
        tkr = yf.Ticker(ticker)
        tz = pytz.timezone('Africa/Johannesburg')
        now = datetime.now(tz)
        start_date = tz.localize(datetime(now.year, 1, 1))
        end_date = start_date + timedelta(days=30)
        buffer_start = start_date - timedelta(days=14)
        data = tkr.history(start=buffer_start, end=end_date, interval="1d")
        if not data.empty:
            data.index = data.index.tz_convert(tz)
            ytd_data = data[data.index >= start_date]
            if not ytd_data.empty:
                price = ytd_data['Close'].iloc[0]
                if price is None or price < 10:
                    return None
                return price
        return None
    except Exception as e:
        print(f"⚠️ YTD reference price error for {ticker}: {str(e)}")
        return None

def get_bitcoin_ytd_price(cg: CoinGeckoAPI) -> Optional[float]:
    try:
        current_year = datetime.now(timezone.utc).year
        start_date = datetime(current_year, 1, 1, tzinfo=timezone.utc)
        end_date = start_date + timedelta(days=1)
        history = cg.get_coin_market_chart_range_by_id("bitcoin", "zar", int(start_date.timestamp()), int(end_date.timestamp()))
        return history['prices'][0][1] if history.get('prices') else None
    except Exception as e:
        print(f"⚠️ Bitcoin YTD error: {str(e)}")
        return None

def fetch_bitcoin_historical(cg: CoinGeckoAPI, days: int) -> Optional[float]:
    try:
        now = datetime.now(timezone.utc)
        target_date = now - timedelta(days=days)
        window = timedelta(hours=12)
        history = cg.get_coin_market_chart_range_by_id("bitcoin", "zar", int((target_date - window).timestamp()), int((target_date + window).timestamp()))
        prices = history.get("prices", [])
        if not prices:
            return None
        target_ts = target_date.timestamp() * 1000
        closest_price = min(prices, key=lambda x: abs(x[0] - target_ts))
        return closest_price[1]
    except Exception as e:
        print(f"⚠️ Bitcoin historical data error for {days} days: {str(e)}")
        return None

def get_latest_price(ticker: str) -> Optional[float]:
    try:
        stock = yf.Ticker(ticker)
        data = stock.history(period="2d", interval="1d")
        price = data['Close'].iloc[-1] if not data.empty else None
        if price is None or price < 10:
            return None
        return price
    except Exception as e:
        print(f"⚠️ Price fetch error for {ticker}: {str(e)}")
        return None

def fetch_market_data() -> Optional[Dict[str, Any]]:
    cg = CoinGeckoAPI()
    tz = pytz.timezone("Africa/Johannesburg")
    now = datetime.now(tz)

    try:
        jse_tickers = ["^J203.JO", "J203.JO", "JALSHARES.JO"]
        jse = None
        jse_ticker_used = None
        for ticker in jse_tickers:
            jse = get_latest_price(ticker)
            if jse is not None:
                jse_ticker_used = ticker
                break

        if jse is None:
            print("⚠️ Could not fetch JSE All Share data from any ticker")
            return None

        forex = {k: get_latest_price(k) for k in ["ZAR=X", "EURZAR=X", "GBPZAR=X"]}
        commodities = {k: get_latest_price(k) for k in ["BZ=F", "GC=F"]}
        indices = {"^GSPC": get_latest_price("^GSPC")}

        try:
            bitcoin_now = cg.get_price(ids="bitcoin", vs_currencies="zar")["bitcoin"]["zar"]
        except Exception as e:
            print(f"⚠️ Bitcoin current price error: {str(e)}")
            bitcoin_now = None

        results = {
            "timestamp": now.strftime("%Y-%m-%d %H:%M"),
            "JSEALSHARE": {
                "Today": jse,
                "Change": calculate_percentage(fetch_historical(jse_ticker_used, 1), jse),
                "Monthly": calculate_percentage(fetch_historical(jse_ticker_used, 30), jse),
                "YTD": calculate_percentage(get_ytd_reference_price(jse_ticker_used), jse)
            },
            "USDZAR": {
                "Today": forex["ZAR=X"],
                "Change": calculate_percentage(fetch_historical("ZAR=X", 1), forex["ZAR=X"]),
                "Monthly": calculate_percentage(fetch_historical("ZAR=X", 30), forex["ZAR=X"]),
                "YTD": calculate_percentage(get_ytd_reference_price("ZAR=X"), forex["ZAR=X"])
            },
            "EURZAR": {
                "Today": forex["EURZAR=X"],
                "Change": calculate_percentage(fetch_historical("EURZAR=X", 1), forex["EURZAR=X"]),
                "Monthly": calculate_percentage(fetch_historical("EURZAR=X", 30), forex["EURZAR=X"]),
                "YTD": calculate_percentage(get_ytd_reference_price("EURZAR=X"), forex["EURZAR=X"])
            },
            "GBPZAR": {
                "Today": forex["GBPZAR=X"],
                "Change": calculate_percentage(fetch_historical("GBPZAR=X", 1), forex["GBPZAR=X"]),
                "Monthly": calculate_percentage(fetch_historical("GBPZAR=X", 30), forex["GBPZAR=X"]),
                "YTD": calculate_percentage(get_ytd_reference_price("GBPZAR=X"), forex["GBPZAR=X"])
            },
            "BRENT": {
                "Today": commodities["BZ=F"],
                "Change": calculate_percentage(fetch_historical("BZ=F", 1), commodities["BZ=F"]),
                "Monthly": calculate_percentage(fetch_historical("BZ=F", 30), commodities["BZ=F"]),
                "YTD": calculate_percentage(get_ytd_reference_price("BZ=F"), commodities["BZ=F"])
            },
            "GOLD": {
                "Today": commodities["GC=F"],
                "Change": calculate_percentage(fetch_historical("GC=F", 1), commodities["GC=F"]),
                "Monthly": calculate_percentage(fetch_historical("GC=F", 30), commodities["GC=F"]),
                "YTD": calculate_percentage(get_ytd_reference_price("GC=F"), commodities["GC=F"])
            },
            "SP500": {
                "Today": indices["^GSPC"],
                "Change": calculate_percentage(fetch_historical("^GSPC", 1), indices["^GSPC"]),
                "Monthly": calculate_percentage(fetch_historical("^GSPC", 30), indices["^GSPC"]),
                "YTD": calculate_percentage(get_ytd_reference_price("^GSPC"), indices["^GSPC"])
            },
            "BITCOINZAR": {
                "Today": bitcoin_now,
                "Change": calculate_percentage(fetch_bitcoin_historical(cg, 1), bitcoin_now),
                "Monthly": calculate_percentage(fetch_bitcoin_historical(cg, 30), bitcoin_now),
                "YTD": calculate_percentage(get_bitcoin_ytd_price(cg), bitcoin_now)
            }
        }

        return results

    except Exception as e:
        print(f"❌ Critical error in fetch_market_data: {str(e)}")
        return None

if __name__ == "__main__":
    data = fetch_market_data()
    if data:
        print("🚀 Market data fetched successfully:")
        print(data)
    else:
        print("❌ Failed to fetch market data")
