from typing import Optional, Dict, Any, Tuple
import yfinance as yf
from pycoingecko import CoinGeckoAPI
from datetime import datetime, timezone, timedelta
import pytz
import logging
import json
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Persistent cache for historical lookups
CACHE_FILE = "historical_cache.json"
try:
    with open(CACHE_FILE, "r") as f:
        _HIST_CACHE: Dict[str, float] = json.load(f)
except FileNotFoundError:
    _HIST_CACHE = {}

def save_cache():
    with open(CACHE_FILE, "w") as f:
        json.dump(_HIST_CACHE, f)

def make_cache_key(ticker: str, days: int) -> str:
    return f"{ticker}_{days}"

def calculate_percentage(old: Optional[float], new: Optional[float]) -> float:
    if None in (old, new) or old == 0:
        return 0.0
    try:
        return ((new - old) / old) * 100
    except (TypeError, ZeroDivisionError):
        return 0.0

def fetch_historical(ticker: str, days: int) -> Optional[float]:
    cache_key = make_cache_key(ticker, days)
    if cache_key in _HIST_CACHE:
        return _HIST_CACHE[cache_key]

    try:
        buffer_days = max(10, days * 2)
        stock = yf.Ticker(ticker)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days + buffer_days)
        df = stock.history(start=start_date, end=end_date, interval="1d")

        if len(df) >= days + 1:
            result = df['Close'].iloc[-days - 1]
            _HIST_CACHE[cache_key] = result
            save_cache()
            return result

        logging.warning(f"Insufficient data for {ticker} ({days}d)")
        return None
    except Exception as e:
        logging.error(f"Historical error for {ticker}: {e}")
        return None

def get_ytd_reference_price(ticker: str) -> Optional[float]:
    try:
        tz = pytz.timezone('Africa/Johannesburg')
        now = datetime.now(tz)
        year_start = tz.localize(datetime(now.year, 1, 1))
        start_date = year_start - timedelta(days=21)
        end_date = year_start + timedelta(days=21)

        df = yf.Ticker(ticker).history(start=start_date, end=end_date, interval="1d")
        if not df.empty:
            if df.index.tz is None:
                df.index = df.index.tz_localize("UTC")
            df.index = df.index.tz_convert(tz)
            ytd_df = df[df.index >= year_start]
            if not ytd_df.empty:
                return ytd_df['Close'].iloc[0]

        logging.warning(f"No YTD data for {ticker}")
        return None
    except Exception as e:
        logging.error(f"YTD error for {ticker}: {e}")
        return None

def get_latest_price(ticker: str) -> Optional[float]:
    try:
        stock = yf.Ticker(ticker)
        if hasattr(stock, 'fast_info') and stock.fast_info.last_price is not None:
            return stock.fast_info.last_price
        if 'regularMarketPrice' in stock.info:
            return stock.info['regularMarketPrice']
        df = stock.history(period="1d", interval="1d")
        if not df.empty:
            return df["Close"].iloc[-1]
        logging.warning(f"No price data for {ticker}")
        return None
    except Exception as e:
        logging.error(f"Price error for {ticker}: {e}")
        return None

def fetch_bitcoin_historical(cg: CoinGeckoAPI, days: int) -> Optional[float]:
    try:
        now = datetime.now(timezone.utc)
        target = now - timedelta(days=days)
        window = timedelta(hours=12)
        history = cg.get_coin_market_chart_range_by_id(
            "bitcoin", "zar",
            int((target - window).timestamp()),
            int((target + window).timestamp())
        )
        prices = history.get("prices", [])
        if not prices:
            return None
        target_ts = target.timestamp() * 1000
        closest = min(prices, key=lambda x: abs(x[0] - target_ts))
        return closest[1]
    except Exception as e:
        logging.error(f"Bitcoin historical data error for {days} days: {e}")
        return None

def get_bitcoin_ytd_price(cg: CoinGeckoAPI) -> Optional[float]:
    try:
        year = datetime.now(timezone.utc).year
        start = datetime(year, 1, 1, tzinfo=timezone.utc)
        end = start + timedelta(days=1)
        history = cg.get_coin_market_chart_range_by_id(
            "bitcoin", "zar",
            int(start.timestamp()),
            int(end.timestamp())
        )
        return history['prices'][0][1] if history.get('prices') else None
    except Exception as e:
        logging.error(f"Bitcoin YTD error: {e}")
        return None

def fetch_market_data() -> Optional[Dict[str, Any]]:
    cg = CoinGeckoAPI()
    sast_time = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=2)))
    report_hour = 17 if sast_time.hour >= 15 else 5
    report_time = sast_time.replace(hour=report_hour, minute=0)

    try:
        jse_tickers = ["^J203.JO", "J203.JO"]
        jse_price, jse_ticker = None, None
        for ticker in jse_tickers:
            price = get_latest_price(ticker)
            if price:
                jse_price, jse_ticker = price, ticker
                break

        if not jse_price:
            logging.error("Failed to fetch JSE All Share")
            return None

        prices = {
            "ZAR=X": get_latest_price("ZAR=X"),
            "EURZAR=X": get_latest_price("EURZAR=X"),
            "GBPZAR=X": get_latest_price("GBPZAR=X"),
            "BZ=F": get_latest_price("BZ=F"),
            "GC=F": get_latest_price("GC=F"),
            "^GSPC": get_latest_price("^GSPC")
        }

        try:
            bitcoin_price = cg.get_price(ids="bitcoin", vs_currencies="zar")["bitcoin"]["zar"]
        except Exception as e:
            logging.error(f"Bitcoin price error: {e}")
            bitcoin_price = None

        result = {
            "timestamp": report_time.strftime("%Y-%m-%d %H:%M"),
            "JSEALSHARE": {
                "Today": jse_price,
                "Change": calculate_percentage(fetch_historical(jse_ticker, 1), jse_price),
                "Monthly": calculate_percentage(fetch_historical(jse_ticker, 30), jse_price),
                "YTD": calculate_percentage(get_ytd_reference_price(jse_ticker), jse_price),
            },
            "USDZAR": {
                "Today": 1 / prices["ZAR=X"] if prices["ZAR=X"] else None,
                "Change": calculate_percentage(1 / fetch_historical("ZAR=X", 1), 1 / prices["ZAR=X"]) if prices["ZAR=X"] else None,
                "Monthly": calculate_percentage(1 / fetch_historical("ZAR=X", 30), 1 / prices["ZAR=X"]) if prices["ZAR=X"] else None,
                "YTD": calculate_percentage(1 / get_ytd_reference_price("ZAR=X"), 1 / prices["ZAR=X"]) if prices["ZAR=X"] else None,
            },
            "BITCOINZAR": {
                "Today": bitcoin_price,
                "Change": calculate_percentage(fetch_bitcoin_historical(cg, 1), bitcoin_price),
                "Monthly": calculate_percentage(fetch_bitcoin_historical(cg, 30), bitcoin_price),
                "YTD": calculate_percentage(get_bitcoin_ytd_price(cg), bitcoin_price),
            }
        }

        return result

    except Exception as e:
        logging.critical(f"Fatal error in fetch_market_data: {e}")
        return None
