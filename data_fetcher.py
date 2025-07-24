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

        # Filter out anomalously low values
        df = df[df['Close'] > 50]

        if df.empty:
            logging.warning(f"No valid data for {ticker} ({days}d)")
            return None

        # Detect early morning before markets update and fallback
        tz = pytz.timezone('Africa/Johannesburg')
        now_hour = datetime.now(tz).hour
        if days == 1 and now_hour < 9 and len(df) >= 2:
            result = df['Close'].iloc[-2]  # fallback to two days ago
        elif len(df) >= days:
            result = df['Close'].iloc[-days]
        else:
            logging.warning(f"Insufficient data rows for {ticker} ({days}d)")
            return None

        _HIST_CACHE[cache_key] = result
        save_cache()
        return result

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
                price = ytd_df['Close'].iloc[0]
                return price if price > 50 else None

        logging.warning(f"No valid YTD data for {ticker}")
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
            price = df["Close"].iloc[-1]
            return price if price > 50 else None
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
    utc_now = datetime.now(timezone.utc)
    sast_time = utc_now.astimezone(timezone(timedelta(hours=2)))
    report_hour = 17 if sast_time.hour >= 15 else 5
    report_time = sast_time.replace(hour=report_hour, minute=0, second=0, microsecond=0)

    tickers = {
        "JSEALSHARE": ["^J203.JO", "J203.JO"],
        "USDZAR": ["ZAR=X"],
        "EURZAR": ["EURZAR=X"],
        "GBPZAR": ["GBPZAR=X"],
        "BRENT": ["BZ=F"],
        "GOLD": ["GC=F"],
        "SP500": ["^GSPC"]
    }

    prices, historical, ytd = {}, {}, {}

    for key, candidates in tickers.items():
        value = None
        for ticker in candidates:
            value = get_latest_price(ticker)
            if value: break
        prices[key] = value
        hist_1d = fetch_historical(candidates[0], 1)
        hist_30d = fetch_historical(candidates[0], 30)
        ytd_price = get_ytd_reference_price(candidates[0])
        historical[key] = {"1d": hist_1d, "30d": hist_30d}
        ytd[key] = ytd_price

    prices["BITCOINZAR"] = cg.get_price(ids="bitcoin", vs_currencies="zar")["bitcoin"]["zar"]
    historical["BITCOINZAR"] = {
        "1d": fetch_bitcoin_historical(cg, 1),
        "30d": fetch_bitcoin_historical(cg, 30)
    }
    ytd["BITCOINZAR"] = get_bitcoin_ytd_price(cg)

    def build(asset):
        return {
            "Today": prices[asset],
            "Change": calculate_percentage(historical[asset]["1d"], prices[asset]),
            "Monthly": calculate_percentage(historical[asset]["30d"], prices[asset]),
            "YTD": calculate_percentage(ytd[asset], prices[asset])
        }

    return {
        "timestamp": report_time.strftime("%Y-%m-%d %H:%M"),
        "JSEALSHARE": build("JSEALSHARE"),
        "USDZAR": build("USDZAR"),
        "EURZAR": build("EURZAR"),
        "GBPZAR": build("GBPZAR"),
        "BRENT": build("BRENT"),
        "GOLD": build("GOLD"),
        "SP500": build("SP500"),
        "BITCOINZAR": build("BITCOINZAR")
    }
