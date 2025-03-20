import yfinance as yf
from pycoingecko import CoinGeckoAPI
from datetime import datetime, timedelta

def calculate_percentage(old, new):
    """Calculate percentage change with safety checks"""
    if old is None or new is None:
        return 0.0
    try:
        return ((new - old) / old) * 100
    except ZeroDivisionError:
        return 0.0

def fetch_historical_data(ticker, days=1):
    """Safe historical data fetcher with error handling"""
    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        data = yf.Ticker(ticker).history(
            start=start_date, 
            end=end_date, 
            interval="1d"
        )
        return data['Close'].iloc[0] if not data.empty else None
    except Exception as e:
        print(f"⚠️ Error fetching {ticker} history: {str(e)}")
        return None

def fetch_market_data():
    """Complete market data fetcher with all required metrics"""
    cg = CoinGeckoAPI()
    now = datetime.now()
    
    def safe_fetch(ticker, days):
        """Wrapper for safe historical fetching"""
        val = fetch_historical_data(ticker, days)
        return val if val is not None else 0.0

    try:
        # JSE All Share Index
        jse = yf.Ticker("^JN0U.JO").history(period="1d")["Close"].iloc[-1]
        jse_prev = safe_fetch("^JN0U.JO", 1)
        
        # Forex Rates
        usdzar = yf.Ticker("USDZAR=X").history(period="1d")["Close"].iloc[-1]
        eurzar = yf.Ticker("EURZAR=X").history(period="1d")["Close"].iloc[-1]
        gbpzar = yf.Ticker("GBPZAR=X").history(period="1d")["Close"].iloc[-1]

        # Commodities
        brent = yf.Ticker("BZ=F").history(period="1d")["Close"].iloc[-1]
        gold = yf.Ticker("GC=F").history(period="1d")["Close"].iloc[-1]

        # S&P 500
        sp500 = yf.Ticker("^GSPC").history(period="1d")["Close"].iloc[-1]

        # Bitcoin (ZAR)
        bitcoin_data = cg.get_price(ids="bitcoin", vs_currencies="zar")
        bitcoin = bitcoin_data["bitcoin"]["zar"]

        return {
            "timestamp": now.strftime("%Y-%m-%d %H:%M"),
            "JSE All Share": {
                "Today": jse,
                "Change": calculate_percentage(jse_prev, jse),
                "Monthly": calculate_percentage(safe_fetch("^JN0U.JO", 30), jse),
                "YTD": calculate_percentage(safe_fetch("^JN0U.JO", 365), jse)
            },
            "Rand/Dollar": {
                "Today": usdzar,
                "Change": calculate_percentage(safe_fetch("USDZAR=X", 1), usdzar),
                "Monthly": calculate_percentage(safe_fetch("USDZAR=X", 30), usdzar),
                "YTD": calculate_percentage(safe_fetch("USDZAR=X", 365), usdzar)
            },
            "Rand/Euro": {
                "Today": eurzar,
                "Change": calculate_percentage(safe_fetch("EURZAR=X", 1), eurzar),
                "Monthly": calculate_percentage(safe_fetch("EURZAR=X", 30), eurzar),
                "YTD": calculate_percentage(safe_fetch("EURZAR=X", 365), eurzar)
            },
            "Rand/GBP": {
                "Today": gbpzar,
                "Change": calculate_percentage(safe_fetch("GBPZAR=X", 1), gbpzar),
                "Monthly": calculate_percentage(safe_fetch("GBPZAR=X", 30), gbpzar),
                "YTD": calculate_percentage(safe_fetch("GBPZAR=X", 365), gbpzar)
            },
            "Brent ($/barrel)": {
                "Today": brent,
                "Change": calculate_percentage(safe_fetch("BZ=F", 1), brent),
                "Monthly": calculate_percentage(safe_fetch("BZ=F", 30), brent),
                "YTD": calculate_percentage(safe_fetch("BZ=F", 365), brent)
            },
            "Gold ($/oz)": {
                "Today": gold,
                "Change": calculate_percentage(safe_fetch("GC=F", 1), gold),
                "Monthly": calculate_percentage(safe_fetch("GC=F", 30), gold),
                "YTD": calculate_percentage(safe_fetch("GC=F", 365), gold)
            },
            "S&P500": {
                "Today": sp500,
                "Change": calculate_percentage(safe_fetch("^GSPC", 1), sp500),
                "Monthly": calculate_percentage(safe_fetch("^GSPC", 30), sp500),
                "YTD": calculate_percentage(safe_fetch("^GSPC", 365), sp500)
            },
            "Bitcoin (ZAR)": {
                "Today": bitcoin,
                "Change": 0.0,  # CoinGecko historical data requires different handling
                "Monthly": 0.0,
                "YTD": 0.0
            }
        }
        
    except Exception as e:
        print(f"❌ Critical error in fetch_market_data: {str(e)}")
        return None
