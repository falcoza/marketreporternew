import yfinance as yf
from pycoingecko import CoinGeckoAPI
from datetime import datetime, timezone, timedelta
import pytz
from typing import Optional, Dict, Any, Tuple
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Cache for historical data
_YF_HIST_CACHE: Dict[Tuple[str, int], float] = {}
_BTC_HIST_CACHE: Dict[int, float] = {}

def calculate_percentage(old: Optional[float], new: Optional[float]) -> float:
    """Calculate percentage change with null safety and type hints."""
    if None in (old, new) or old == 0:
        return 0.0
    try:
        return ((new - old) / old) * 100
    except (TypeError, ZeroDivisionError):
        return 0.0

def fetch_historical(ticker: str, days: int) -> Optional[float]:
    """Get historical price with caching and improved date handling."""
    cache_key = (ticker, days)
    if cache_key in _YF_HIST_CACHE:
        return _YF_HIST_CACHE[cache_key]
    
    try:
        buffer_days = max(5, days // 5)
        stock = yf.Ticker(ticker)
        df = stock.history(period=f"{days + buffer_days}d", interval="1d")
        
        if not df.empty and len(df) >= days + 1:
            result = df['Close'].iloc[-days-1]
            _YF_HIST_CACHE[cache_key] = result
            return result
        return None
    except Exception as e:
        logging.error(f"Historical data error for {ticker}: {e}", exc_info=True)
        return None

def get_ytd_reference_price(ticker: str) -> Optional[float]:
    """Fetch the first trading day's closing price of current year."""
    try:
        stock = yf.Ticker(ticker)
        tz = pytz.timezone('Africa/Johannesburg')
        now = datetime.now(tz)
        start = tz.localize(datetime(now.year, 1, 1))
        end = start + timedelta(days=30)
        buffer_start = start - timedelta(days=14)

        df = stock.history(start=buffer_start, end=end, interval="1d")
        if not df.empty:
            df.index = df.index.tz_convert(tz)
            ytd = df[df.index >= start]
            if not ytd.empty:
                return ytd['Close'].iloc[0]
        return None
    except Exception as e:
        logging.error(f"YTD reference price error for {ticker}: {e}", exc_info=True)
        return None

def get_bitcoin_ytd_price(cg: CoinGeckoAPI) -> Optional[float]:
    """Get Bitcoin price on Jan 1 of current year."""
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
        logging.error(f"Bitcoin YTD error: {e}", exc_info=True)
        return None

def fetch_bitcoin_historical(cg: CoinGeckoAPI, days: int) -> Optional[float]:
    """Fetch historical Bitcoin price with caching."""
    if days in _BTC_HIST_CACHE:
        return _BTC_HIST_CACHE[days]
    
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
        _BTC_HIST_CACHE[days] = closest[1]
        return closest[1]
    except Exception as e:
        logging.error(f"Bitcoin historical error ({days} days): {e}", exc_info=True)
        return None

def get_latest_price(ticker: str) -> Optional[float]:
    """Fetch latest price with improved error handling."""
    try:
        stock = yf.Ticker(ticker)
        
        # Try different methods with priority
        if hasattr(stock, 'fast_info') and stock.fast_info.last_price is not None:
            return stock.fast_info.last_price
            
        if 'regularMarketPrice' in stock.info:
            return stock.info['regularMarketPrice']
            
        # Fallback to history
        df = stock.history(period="1d", interval="1d")
        if not df.empty:
            return df["Close"].iloc[-1]
            
        logging.warning(f"No price data available for {ticker}")
        return None
    except Exception as e:
        logging.error(f"Price fetch error for {ticker}: {e}", exc_info=True)
        return None

def fetch_market_data() -> Optional[Dict[str, Any]]:
    """Main function with optimized API calls and error handling."""
    cg = CoinGeckoAPI()
    sast_time = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=2)))
    report_time = sast_time.replace(hour=17 if sast_time.hour >= 15 else 5, minute=0)

    try:
        # Fetch JSE All-Share Index
        jse_ticker = "^J203.JO"
        jse = get_latest_price(jse_ticker)
        if jse is None:
            logging.error("Could not fetch JSE All-Share Index")
            return None

        # Fetch all current prices first
        assets = {
            "usdzar": "ZAR=X",
            "eurzar": "EURZAR=X",
            "gbpzar": "GBPZAR=X",
            "brent": "BZ=F",
            "gold": "GC=F",
            "sp500": "^GSPC"
        }
        current_prices = {name: get_latest_price(ticker) for name, ticker in assets.items()}
        
        # Fetch Bitcoin price
        try:
            bitcoin_data = cg.get_price(ids="bitcoin", vs_currencies="zar")
            current_prices["bitcoin"] = bitcoin_data["bitcoin"]["zar"]
        except Exception as e:
            logging.error(f"Bitcoin current price error: {e}", exc_info=True)
            current_prices["bitcoin"] = None

        # Fetch historical data in bulk
        hist_1d = {name: fetch_historical(ticker, 1) for name, ticker in assets.items()}
        hist_30d = {name: fetch_historical(ticker, 30) for name, ticker in assets.items()}
        
        # Fetch JSE historical
        jse_1d = fetch_historical(jse_ticker, 1)
        jse_30d = fetch_historical(jse_ticker, 30)
        
        # Fetch Bitcoin historical
        btc_1d = fetch_bitcoin_historical(cg, 1)
        btc_30d = fetch_bitcoin_historical(cg, 30)
        
        # Fetch YTD prices
        ytd_prices = {
            "jse": get_ytd_reference_price(jse_ticker),
            "usdzar": get_ytd_reference_price("ZAR=X"),
            "eurzar": get_ytd_reference_price("EURZAR=X"),
            "gbpzar": get_ytd_reference_price("GBPZAR=X"),
            "brent": get_ytd_reference_price("BZ=F"),
            "gold": get_ytd_reference_price("GC=F"),
            "sp500": get_ytd_reference_price("^GSPC"),
            "bitcoin": get_bitcoin_ytd_price(cg)
        }

        # Build result structure
        result = {
            "timestamp": report_time.strftime("%Y-%m-%d %H:%M"),
            "JSEALSHARE": {
                "Today": jse,
                "Change": calculate_percentage(jse_1d, jse),
                "Monthly": calculate_percentage(jse_30d, jse),
                "YTD": calculate_percentage(ytd_prices["jse"], jse)
            },
            "USDZAR": {
                "Today": current_prices["usdzar"],
                "Change": calculate_percentage(hist_1d["usdzar"], current_prices["usdzar"]),
                "Monthly": calculate_percentage(hist_30d["usdzar"], current_prices["usdzar"]),
                "YTD": calculate_percentage(ytd_prices["usdzar"], current_prices["usdzar"])
            },
            "EURZAR": {
                "Today": current_prices["eurzar"],
                "Change": calculate_percentage(hist_1d["eurzar"], current_prices["eurzar"]),
                "Monthly": calculate_percentage(hist_30d["eurzar"], current_prices["eurzar"]),
                "YTD": calculate_percentage(ytd_prices["eurzar"], current_prices["eurzar"])
            },
            "GBPZAR": {
                "Today": current_prices["gbpzar"],
                "Change": calculate_percentage(hist_1d["gbpzar"], current_prices["gbpzar"]),
                "Monthly": calculate_percentage(hist_30d["gbpzar"], current_prices["gbpzar"]),
                "YTD": calculate_percentage(ytd_prices["gbpzar"], current_prices["gbpzar"])
            },
            "BRENT": {
                "Today": current_prices["brent"],
                "Change": calculate_percentage(hist_1d["brent"], current_prices["brent"]),
                "Monthly": calculate_percentage(hist_30d["brent"], current_prices["brent"]),
                "YTD": calculate_percentage(ytd_prices["brent"], current_prices["brent"])
            },
            "GOLD": {
                "Today": current_prices["gold"],
                "Change": calculate_percentage(hist_1d["gold"], current_prices["gold"]),
                "Monthly": calculate_percentage(hist_30d["gold"], current_prices["gold"]),
                "YTD": calculate_percentage(ytd_prices["gold"], current_prices["gold"])
            },
            "SP500": {
                "Today": current_prices["sp500"],
                "Change": calculate_percentage(hist_1d["sp500"], current_prices["sp500"]),
                "Monthly": calculate_percentage(hist_30d["sp500"], current_prices["sp500"]),
                "YTD": calculate_percentage(ytd_prices["sp500"], current_prices["sp500"])
            },
            "BITCOINZAR": {
                "Today": current_prices["bitcoin"],
                "Change": calculate_percentage(btc_1d, current_prices["bitcoin"]),
                "Monthly": calculate_percentage(btc_30d, current_prices["bitcoin"]),
                "YTD": calculate_percentage(ytd_prices["bitcoin"], current_prices["bitcoin"])
            }
        }

        return result

    except Exception as e:
        logging.critical(f"Critical error in fetch_market_data: {e}", exc_info=True)
        return None

if __name__ == "__main__":
    data = fetch_market_data()
    if data:
        print("🚀 Market data fetched successfully:")
        print(data)
    else:
        print("❌ Failed to fetch market data")
