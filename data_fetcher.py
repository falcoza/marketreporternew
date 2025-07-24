from typing import Optional, Dict, Any, Tuple
import yfinance as yf
from pycoingecko import CoinGeckoAPI
from datetime import datetime, timezone, timedelta
import pytz
import logging
import json
import os
from tenacity import retry, stop_after_attempt, wait_exponential

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Persistent cache for historical lookups
CACHE_FILE = "historical_cache.json"
_HIST_CACHE = {}
try:
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r") as f:
            _HIST_CACHE = json.load(f)
    logging.info(f"Loaded cache with {len(_HIST_CACHE)} entries")
except Exception as e:
    logging.error(f"Cache load error: {e}")
    _HIST_CACHE = {}

def save_cache():
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump(_HIST_CACHE, f)
        logging.info(f"Cache saved with {len(_HIST_CACHE)} entries")
    except Exception as e:
        logging.error(f"Cache save error: {e}")

def make_cache_key(ticker: str, days: int) -> str:
    date_str = datetime.now().strftime("%Y%m%d")
    return f"{date_str}_{ticker}_{days}"

def calculate_percentage(old: Optional[float], new: Optional[float]) -> float:
    if None in (old, new) or old == 0:
        return 0.0
    try:
        return ((new - old) / old) * 100
    except (TypeError, ZeroDivisionError):
        return 0.0

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
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

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def get_ytd_reference_price(ticker: str) -> Optional[float]:
    try:
        tz = pytz.timezone('Africa/Johannesburg')
        now = datetime.now(tz)
        year_start = tz.localize(datetime(now.year, 1, 1))  # Fixed syntax error here
        cache_key = f"YTD_{ticker}_{now.year}"
        
        if cache_key in _HIST_CACHE:
            return _HIST_CACHE[cache_key]
        
        start_date = year_start - timedelta(days=21)
        end_date = year_start + timedelta(days=21)

        df = yf.Ticker(ticker).history(start=start_date, end=end_date, interval="1d")
        if not df.empty:
            if df.index.tz is None:
                df.index = df.index.tz_localize("UTC")
            df.index = df.index.tz_convert(tz)
            ytd_df = df[df.index >= year_start]
            if not ytd_df.empty:
                result = ytd_df['Close'].iloc[0]
                _HIST_CACHE[cache_key] = result
                save_cache()
                return result

        logging.warning(f"No YTD data for {ticker}")
        return None
    except Exception as e:
        logging.error(f"YTD error for {ticker}: {e}")
        return None

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=5))
def get_latest_price(ticker: str) -> Optional[float]:
    try:
        stock = yf.Ticker(ticker)
        if hasattr(stock, 'fast_info') and stock.fast_info.last_price is not None:
            return stock.fast_info.last_price
        if 'currentPrice' in stock.info:
            return stock.info['currentPrice']
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

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=5))
def fetch_bitcoin_historical(cg: CoinGeckoAPI, days: int) -> Optional[float]:
    cache_key = f"BTC_{days}_{datetime.now().date()}"
    if cache_key in _HIST_CACHE:
        return _HIST_CACHE[cache_key]
    
    try:
        now = datetime.now(timezone.utc)
        target = now - timedelta(days=days)
        window = timedelta(hours=24)  # Increased window for reliability
        
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
        _HIST_CACHE[cache_key] = closest[1]
        save_cache()
        return closest[1]
    except Exception as e:
        logging.error(f"Bitcoin historical error for {days} days: {e}")
        return None

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=5))
def get_bitcoin_ytd_price(cg: CoinGeckoAPI) -> Optional[float]:
    cache_key = f"BTC_YTD_{datetime.now().year}"
    if cache_key in _HIST_CACHE:
        return _HIST_CACHE[cache_key]
    
    try:
        year = datetime.now(timezone.utc).year
        start = datetime(year, 1, 1, tzinfo=timezone.utc)
        end = start + timedelta(days=3)  # 3-day window
        
        history = cg.get_coin_market_chart_range_by_id(
            "bitcoin", "zar",
            int(start.timestamp()),
            int(end.timestamp())
        )
        if not history.get('prices'):
            return None
            
        # Find first price after Jan 1
        for ts, price in history['prices']:
            if datetime.fromtimestamp(ts/1000, tz=timezone.utc).date() >= start.date():
                _HIST_CACHE[cache_key] = price
                save_cache()
                return price
                
        return None
    except Exception as e:
        logging.error(f"Bitcoin YTD error: {e}")
        return None

def fetch_market_data() -> Optional[Dict[str, Any]]:
    cg = CoinGeckoAPI()
    sast_time = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=2)))
    report_hour = 17 if sast_time.hour >= 15 else 5
    report_time = sast_time.replace(hour=report_hour, minute=0)

    try:
        # JSE All Share
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

        # Current prices
        prices = {
            "ZAR=X": get_latest_price("ZAR=X"),
            "EURZAR=X": get_latest_price("EURZAR=X"),
            "GBPZAR=X": get_latest_price("GBPZAR=X"),
            "BZ=F": get_latest_price("BZ=F"),
            "GC=F": get_latest_price("GC=F"),
            "^GSPC": get_latest_price("^GSPC")
        }

        # Bitcoin price
        try:
            bitcoin_price = cg.get_price(ids="bitcoin", vs_currencies="zar")["bitcoin"]["zar"]
        except Exception as e:
            logging.error(f"Bitcoin price error: {e}")
            bitcoin_price = None

        # Pre-fetch all historical data to avoid redundant calculations
        hist_data = {
            "jse_1d": fetch_historical(jse_ticker, 1),
            "jse_30d": fetch_historical(jse_ticker, 30),
            "jse_ytd": get_ytd_reference_price(jse_ticker),
            "zarusd_1d": fetch_historical("ZAR=X", 1),
            "zarusd_30d": fetch_historical("ZAR=X", 30),
            "zarusd_ytd": get_ytd_reference_price("ZAR=X"),
            "eurzar_1d": fetch_historical("EURZAR=X", 1),
            "eurzar_30d": fetch_historical("EURZAR=X", 30),
            "eurzar_ytd": get_ytd_reference_price("EURZAR=X"),
            "gbpzar_1d": fetch_historical("GBPZAR=X", 1),
            "gbpzar_30d": fetch_historical("GBPZAR=X", 30),
            "gbpzar_ytd": get_ytd_reference_price("GBPZAR=X"),
            "brent_1d": fetch_historical("BZ=F", 1),
            "brent_30d": fetch_historical("BZ=F", 30),
            "brent_ytd": get_ytd_reference_price("BZ=F"),
            "gold_1d": fetch_historical("GC=F", 1),
            "gold_30d": fetch_historical("GC=F", 30),
            "gold_ytd": get_ytd_reference_price("GC=F"),
            "sp500_1d": fetch_historical("^GSPC", 1),
            "sp500_30d": fetch_historical("^GSPC", 30),
            "sp500_ytd": get_ytd_reference_price("^GSPC"),
            "btc_1d": fetch_bitcoin_historical(cg, 1),
            "btc_30d": fetch_bitcoin_historical(cg, 30),
            "btc_ytd": get_bitcoin_ytd_price(cg)
        }

        # Build result
        return {
            "timestamp": report_time.strftime("%Y-%m-%d %H:%M"),
            "JSEALSHARE": {
                "Today": jse_price,
                "Change": calculate_percentage(hist_data["jse_1d"], jse_price),
                "Monthly": calculate_percentage(hist_data["jse_30d"], jse_price),
                "YTD": calculate_percentage(hist_data["jse_ytd"], jse_price),
            },
            "USDZAR": {
                "Today": prices["ZAR=X"],
                "Change": calculate_percentage(hist_data["zarusd_1d"], prices["ZAR=X"]),
                "Monthly": calculate_percentage(hist_data["zarusd_30d"], prices["ZAR=X"]),
                "YTD": calculate_percentage(hist_data["zarusd_ytd"], prices["ZAR=X"]),
            },
            "EURZAR": {
                "Today": prices["EURZAR=X"],
                "Change": calculate_percentage(hist_data["eurzar_1d"], prices["EURZAR=X"]),
                "Monthly": calculate_percentage(hist_data["eurzar_30d"], prices["EURZAR=X"]),
                "YTD": calculate_percentage(hist_data["eurzar_ytd"], prices["EURZAR=X"]),
            },
            "GBPZAR": {
                "Today": prices["GBPZAR=X"],
                "Change": calculate_percentage(hist_data["gbpzar_1d"], prices["GBPZAR=X"]),
                "Monthly": calculate_percentage(hist_data["gbpzar_30d"], prices["GBPZAR=X"]),
                "YTD": calculate_percentage(hist_data["gbpzar_ytd"], prices["GBPZAR=X"]),
            },
            "BRENT": {
                "Today": prices["BZ=F"],
                "Change": calculate_percentage(hist_data["brent_1d"], prices["BZ=F"]),
                "Monthly": calculate_percentage(hist_data["brent_30d"], prices["BZ=F"]),
                "YTD": calculate_percentage(hist_data["brent_ytd"], prices["BZ=F"]),
            },
            "GOLD": {
                "Today": prices["GC=F"],
                "Change": calculate_percentage(hist_data["gold_1d"], prices["GC=F"]),
                "Monthly": calculate_percentage(hist_data["gold_30d"], prices["GC=F"]),
                "YTD": calculate_percentage(hist_data["gold_ytd"], prices["GC=F"]),
            },
            "SP500": {
                "Today": prices["^GSPC"],
                "Change": calculate_percentage(hist_data["sp500_1d"], prices["^GSPC"]),
                "Monthly": calculate_percentage(hist_data["sp500_30d"], prices["^GSPC"]),
                "YTD": calculate_percentage(hist_data["sp500_ytd"], prices["^GSPC"]),
            },
            "BITCOINZAR": {
                "Today": bitcoin_price,
                "Change": calculate_percentage(hist_data["btc_1d"], bitcoin_price),
                "Monthly": calculate_percentage(hist_data["btc_30d"], bitcoin_price),
                "YTD": calculate_percentage(hist_data["btc_ytd"], bitcoin_price),
            }
        }

    except Exception as e:
        logging.critical(f"Fatal error in fetch_market_data: {e}", exc_info=True)
        return None
