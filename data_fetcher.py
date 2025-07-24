import yfinance as yf
from pycoingecko import CoinGeckoAPI
from datetime import datetime, timezone, timedelta
import pytz
from typing import Optional, Dict, Any

def calculate_percentage(old: Optional[float], new: Optional[float]) -> float:
    """Calculate percentage change with null safety and type hints"""
    if None in (old, new) or old == 0:
        return 0.0
    try:
        return ((new - old) / old) * 100
    except (TypeError, ZeroDivisionError):
        return 0.0

def fetch_historical(ticker: str, days: int) -> Optional[float]:
    """Get historical price accounting for non-trading days using yfinance"""
    try:
        buffer_days = max(5, days // 5)  # Dynamic buffer for weekends/holidays
        stock = yf.Ticker(ticker)
        data = stock.history(period=f"{days + buffer_days}d", interval="1d")
        if not data.empty and len(data) >= days + 1:
            return data['Close'].iloc[-days-1]
        return None
    except Exception as e:
        print(f"⚠️ Historical data error for {ticker}: {str(e)}")
        return None

def get_ytd_reference_price(ticker: str) -> Optional[float]:
    """Fetch the first trading day's closing price of the current year"""
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
                return ytd_data['Close'].iloc[0]
        return None
    except Exception as e:
        print(f"⚠️ YTD reference price error for {ticker}: {str(e)}")
        return None

def get_bitcoin_ytd_price(cg: CoinGeckoAPI) -> Optional[float]:
    """Get Bitcoin price on Jan 1 of current year using CoinGecko API"""
    try:
        current_year = datetime.now(timezone.utc).year
        start_date = datetime(current_year, 1, 1, tzinfo=timezone.utc)
        end_date = start_date + timedelta(days=1)
        history = cg.get_coin_market_chart_range_by_id(
            "bitcoin", "zar",
            int(start_date.timestamp()),
            int(end_date.timestamp())
        )
        return history['prices'][0][1] if history.get('prices') else None
    except Exception as e:
        print(f"⚠️ Bitcoin YTD error: {str(e)}")
        return None

def fetch_bitcoin_historical(cg: CoinGeckoAPI, days: int) -> Optional[float]:
    """Fetch historical Bitcoin price in ZAR"""
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
        target_ts = target_date.timestamp() * 1000
        closest_price = min(prices, key=lambda x: abs(x[0] - target_ts))
        return closest_price[1]
    except Exception as e:
        print(f"⚠️ Bitcoin historical data error for {days} days: {str(e)}")
        return None

def get_latest_price(ticker: str) -> Optional[float]:
    """Helper function to get latest price with error handling"""
    try:
        stock = yf.Ticker(ticker)
        data = stock.history(period="2d", interval="1d")
        return data['Close'].iloc[-1] if not data.empty else None
    except Exception as e:
        print(f"⚠️ Price fetch error for {ticker}: {str(e)}")
        return None

def fetch_market_data() -> Optional[Dict[str, Any]]:
    """Main function to fetch all market data"""
    cg = CoinGeckoAPI()

    # Time handling with proper timezone conversion
    utc_now = datetime.now(timezone.utc)
    sast_time = utc_now.astimezone(timezone(timedelta(hours=2)))

    # Determine report time
    if utc_now.hour == 3:
        report_time = sast_time.replace(hour=5, minute=0)
    elif utc_now.hour == 15:
        report_time = sast_time.replace(hour=17, minute=0)
    else:
        report_time = sast_time

    try:
        # JSE All‑Share Index tickers (fallbacks)
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

        # Other markets
        zarusd = get_latest_price("ZAR=X")
        eurzar = get_latest_price("EURZAR=X")
        gbpzar = get_latest_price("GBPZAR=X")
        brent  = get_latest_price("BZ=F")
        gold   = get_latest_price("GC=F")
        sp500  = get_latest_price("^GSPC")
        try:
            bitcoin_data = cg.get_price(ids="bitcoin", vs_currencies="zar")
            bitcoin = bitcoin_data["bitcoin"]["zar"]
        except Exception as e:
            print(f"⚠️ Bitcoin current price error: {str(e)}")
            bitcoin = None

        # Historical (1‑day) and YTD
        jse_1d    = fetch_historical(jse_ticker_used, 1) if jse_ticker_used else None
        zarusd_1d = fetch_historical("ZAR=X", 1)
        eurzar_1d = fetch_historical("EURZAR=X", 1)
        gbpzar_1d = fetch_historical("GBPZAR=X", 1)
        brent_1d  = fetch_historical("BZ=F", 1)
        gold_1d   = fetch_historical("GC=F", 1)
        sp500_1d  = fetch_historical("^GSPC", 1)

        jse_ytd   = get_ytd_reference_price(jse_ticker_used) if jse_ticker_used else None
        zarusd_ytd= get_ytd_reference_price("ZAR=X")
        eurzar_ytd= get_ytd_reference_price("EURZAR=X")
        gbpzar_ytd= get_ytd_reference_price("GBPZAR=X")
        brent_ytd = get_ytd_reference_price("BZ=F")
        gold_ytd  = get_ytd_reference_price("GC=F")
        sp500_ytd = get_ytd_reference_price("^GSPC")
        btc_ytd   = get_bitcoin_ytd_price(cg)

        # USDZAR is just ZAR=X inverted logically
        usdzar     = zarusd
        usdzar_1d  = zarusd_1d
        usdzar_ytd = zarusd_ytd

        result = {
            "timestamp": report_time.strftime("%Y-%m-%d %H:%M"),
            "JSEALSHARE": {
                "Today":   jse,
                "Change":  calculate_percentage(jse_1d, jse),
                "Monthly": calculate_percentage(fetch_historical(jse_ticker_used, 30), jse) if jse_ticker_used else None,
                "YTD":     calculate_percentage(jse_ytd, jse)
            },
            "USDZAR": {
                "Today":   usdzar,
                "Change":  calculate_percentage(usdzar_1d, usdzar),
                "Monthly": calculate_percentage(fetch_historical("ZAR=X", 30), usdzar),
                "YTD":     calculate_percentage(usdzar_ytd, usdzar)
            },
            "EURZAR": {
                "Today":   eurzar,
                "Change":  calculate_percentage(eurzar_1d, eurzar),
                "Monthly": calculate_percentage(fetch_historical("EURZAR=X", 30), eurzar),
                "YTD":     calculate_percentage(eurzar_ytd, eurzar)
            },
            "GBPZAR": {
                "Today":   gbpzar,
                "Change":  calculate_percentage(gbpzar_1d, gbpzar),
                "Monthly": calculate_percentage(fetch_historical("GBPZAR=X", 30), gbpzar),
                "YTD":     calculate_percentage(gbpzar_ytd, gbpzar)
            },
            "BRENT": {
                "Today":   brent,
                "Change":  calculate_percentage(brent_1d, brent),
                "Monthly": calculate_percentage(fetch_historical("BZ=F", 30), brent),
                "YTD":     calculate_percentage(brent_ytd, brent)
            },
            "GOLD": {
                "Today":   gold,
                "Change":  calculate_percentage(gold_1d, gold),
                "Monthly": calculate_percentage(fetch_historical("GC=F", 30), gold),
                "YTD":     calculate_percentage(gold_ytd, gold)
            },
            "SP500": {
                "Today":   sp500,
                "Change":  calculate_percentage(sp500_1d, sp500),
                "Monthly": calculate_percentage(fetch_historical("^GSPC", 30), sp500),
                "YTD":     calculate_percentage(sp500_ytd, sp500)
            },
            "BITCOINZAR": {
                "Today":   bitcoin,
                "Change":  calculate_percentage(fetch_bitcoin_historical(cg, 1), bitcoin),
                "Monthly": calculate_percentage(fetch_bitcoin_historical(cg, 30), bitcoin),
                "YTD":     calculate_percentage(btc_ytd, bitcoin)
            }
        }

        return result

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
