import yfinance as yf
from pycoingecko import CoinGeckoAPI
from datetime import datetime, timezone, timedelta
import pytz
from typing import Optional, Dict, Any
import time
import requests


# ---------- shared session for yfinance to reduce empty responses ----------
_YF_SESSION = requests.Session()
_YF_SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0 Safari/537.36"
})


def _retry(n=3, delay=1.0, backoff=2.0):
    """Simple retry decorator for transient Yahoo/HTTP issues."""
    def deco(fn):
        def wrap(*args, **kwargs):
            _delay = delay
            last_exc = None
            for _ in range(n):
                try:
                    return fn(*args, **kwargs)
                except Exception as e:
                    last_exc = e
                    time.sleep(_delay)
                    _delay *= backoff
            # Re-raise last exception to be handled by caller
            raise last_exc
        return wrap
    return deco


def calculate_percentage(old: Optional[float], new: Optional[float]) -> float:
    if None in (old, new) or old == 0:
        return 0.0
    try:
        return ((new - old) / old) * 100
    except (TypeError, ZeroDivisionError):
        return 0.0


@_retry(n=3, delay=0.6, backoff=1.8)
def _yf_history(ticker: str, start: Optional[datetime] = None, end: Optional[datetime] = None,
                period: Optional[str] = None, interval: str = "1d"):
    """Thin wrapper that always uses our session and supports start/end or period."""
    tkr = yf.Ticker(ticker, session=_YF_SESSION)
    if start or end:
        return tkr.history(start=start, end=end, interval=interval, auto_adjust=False)
    return tkr.history(period=period or "7d", interval=interval, auto_adjust=False)


def fetch_historical(ticker: str, days: int) -> Optional[float]:
    """
    Return the closing price 'days' trading days ago for the given ticker.
    - Primary: use period=(days + buffer)d
    - Fallback: explicit start/end window with buffer (handles Yahoo 'possibly delisted' empties)
    """
    try:
        # Primary path - period
        buffer_days = max(20, days * 3)  # larger buffer to ensure enough trading rows
        df = _yf_history(ticker, period=f"{days + buffer_days}d", interval="1d")
        if not df.empty and len(df) >= days + 1:
            return float(df["Close"].iloc[-days - 1])

        # Fallback path - explicit dates
        # Use UTC end to avoid intraday boundary issues; add +1 day to include last session
        utc_now = datetime.now(timezone.utc)
        end = utc_now + timedelta(days=1)
        start = utc_now - timedelta(days=(days + max(30, days * 4)))  # wider safety window
        df = _yf_history(ticker, start=start, end=end, interval="1d")
        if not df.empty and len(df) >= days + 1:
            return float(df["Close"].iloc[-days - 1])

        print(f"⚠️ Historical data empty for {ticker} ({days}d) after fallback.")
        return None
    except Exception as e:
        print(f"⚠️ Historical data error for {ticker} ({days}d): {e}")
        return None


def get_ytd_reference_price(ticker: str) -> Optional[float]:
    """Fetch the first trading day's closing price of the current year (SAST)."""
    try:
        tz = pytz.timezone('Africa/Johannesburg')
        now = datetime.now(tz)
        start_date = datetime(now.year, 1, 1, tzinfo=tz)
        end_date = start_date + timedelta(days=30)
        buffer_start = start_date - timedelta(days=14)

        df = _yf_history(ticker, start=buffer_start.astimezone(timezone.utc),
                          end=end_date.astimezone(timezone.utc), interval="1d")
        if not df.empty:
            # yfinance returns tz-aware index (UTC); convert to SAST for filtering
            df.index = df.index.tz_convert(tz)
            ytd = df[df.index >= start_date]
            if not ytd.empty:
                return float(ytd["Close"].iloc[0])
        return None
    except Exception as e:
        print(f"⚠️ YTD reference price error for {ticker}: {e}")
        return None


def fetch_bitcoin_historical(cg: CoinGeckoAPI, days: int) -> Optional[float]:
    try:
        now = datetime.now(timezone.utc)
        target_date = now - timedelta(days=days)
        window = timedelta(hours=12)
        history = cg.get_coin_market_chart_range_by_id(
            "bitcoin", "zar",
            int((target_date - window).timestamp()),
            int((target_date + window).timestamp())
        )
        prices = history.get("prices", [])
        if not prices:
            return None
        target_ts = target_date.timestamp() * 1000  # ms
        closest = min(prices, key=lambda x: abs(x[0] - target_ts))
        return float(closest[1])
    except Exception as e:
        print(f"⚠️ Bitcoin historical data error for {days} days: {e}")
        return None


def get_bitcoin_ytd_price(cg: CoinGeckoAPI) -> Optional[float]:
    try:
        current_year = datetime.now(timezone.utc).year
        start_date = datetime(current_year, 1, 1, tzinfo=timezone.utc)
        end_date = start_date + timedelta(days=1)
        history = cg.get_coin_market_chart_range_by_id(
            "bitcoin", "zar", int(start_date.timestamp()), int(end_date.timestamp())
        )
        return float(history['prices'][0][1]) if history.get('prices') else None
    except Exception as e:
        print(f"⚠️ Bitcoin YTD error: {e}")
        return None


def get_latest_price(ticker: str) -> Optional[float]:
    """
    More robust 'latest' price:
    1) fast_info.last_price
    2) info['regularMarketPrice']
    3) history(period='2d')
    4) explicit start/end last 7 days
    All with retries and a shared session to avoid empty Yahoo responses.
    """
    try:
        tkr = yf.Ticker(ticker, session=_YF_SESSION)

        # 1) fast_info
        try:
            fi = getattr(tkr, "fast_info", None)
            if fi and getattr(fi, "last_price", None) is not None:
                return float(fi.last_price)
        except Exception:
            pass

        # 2) info regularMarketPrice
        try:
            info = getattr(tkr, "info", {}) or {}
            rmp = info.get("regularMarketPrice")
            if rmp is not None:
                return float(rmp)
        except Exception:
            pass

        # 3) period='2d'
        try:
            df = _yf_history(ticker, period="2d", interval="1d")
            if not df.empty:
                return float(df["Close"].iloc[-1])
        except Exception:
            pass

        # 4) explicit window last 7 calendar days (UTC end with +1 day)
        utc_now = datetime.now(timezone.utc)
        end = utc_now + timedelta(days=1)
        start = utc_now - timedelta(days=10)
        df = _yf_history(ticker, start=start, end=end, interval="1d")
        if not df.empty:
            return float(df["Close"].iloc[-1])

        print(f"⚠️ No price data for {ticker} after all attempts.")
        return None
    except Exception as e:
        print(f"⚠️ Price fetch error for {ticker}: {e}")
        return None


def fetch_jse_historical_yf(days: int) -> Optional[float]:
    """Dedicated helper for JSE ALSH (^J203.JO) with the same hardened logic as fetch_historical."""
    try:
        ticker = "^J203.JO"

        # Primary path - period
        buffer_days = max(20, days * 3)
        df = _yf_history(ticker, period=f"{days + buffer_days}d", interval="1d")
        if not df.empty and len(df) >= days + 1:
            return float(df["Close"].iloc[-days - 1])

        # Fallback path - explicit dates
        utc_now = datetime.now(timezone.utc)
        end = utc_now + timedelta(days=1)
        start = utc_now - timedelta(days=(days + max(30, days * 4)))
        df = _yf_history(ticker, start=start, end=end, interval="1d")
        if not df.empty and len(df) >= days + 1:
            return float(df["Close"].iloc[-days - 1])

        print(f"⚠️ JSE historical empty (^J203.JO, {days}d) after fallback.")
        return None
    except Exception as e:
        print(f"⚠️ JSE YF historical fetch failed: {e}")
        return None


def fetch_market_data() -> Optional[Dict[str, Any]]:
    cg = CoinGeckoAPI()
    tz = pytz.timezone("Africa/Johannesburg")
    now = datetime.now(tz)
    # Make start_of_year tz-aware without localize/naive mismatch
    start_of_year = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)

    try:
        # JSE All Share
        jse = get_latest_price("^J203.JO")
        jse_1d = fetch_jse_historical_yf(1)
        jse_30d = fetch_jse_historical_yf(30)
        jse_ytd = fetch_jse_historical_yf((now - start_of_year).days)

        # FX / Commodities / Index
        forex = {k: get_latest_price(k) for k in ["ZAR=X", "EURZAR=X", "GBPZAR=X"]}
        commodities = {k: get_latest_price(k) for k in ["BZ=F", "GC=F"]}
        indices = {"^GSPC": get_latest_price("^GSPC")}

        # Bitcoin
        try:
            bitcoin_now = cg.get_price(ids="bitcoin", vs_currencies="zar")["bitcoin"]["zar"]
        except Exception as e:
            print(f"⚠️ Bitcoin current price error: {e}")
            bitcoin_now = None

        results = {
            "timestamp": now.strftime("%Y-%m-%d %H:%M"),
            "JSEALSHARE": {
                "Today": jse,
                "Change": calculate_percentage(jse_1d, jse),
                "Monthly": calculate_percentage(jse_30d, jse),
                "YTD": calculate_percentage(jse_ytd, jse)
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
        print(f"❌ Critical error in fetch_market_data: {e}")
        return None


if __name__ == "__main__":
    data = fetch_market_data()
    if data:
        print("🚀 Market data fetched successfully:")
        print(data)
    else:
        print("❌ Failed to fetch market data")
