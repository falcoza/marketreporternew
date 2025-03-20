import yfinance as yf
from pycoingecko import CoinGeckoAPI
from datetime import datetime, timedelta

def calculate_percentage(old, new):
    """Calculate percentage change safely"""
    if old is None or new is None or old == 0:
        return 0.0
    return ((new - old) / old) * 100

def fetch_historical_data(ticker, days=1):
    """Fetch historical price data"""
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
        print(f"⚠️ Error fetching {ticker}: {str(e)}")
        return None

def get_bitcoin_history(cg, days):
    """Get Bitcoin historical price"""
    try:
        history = cg.get_coin_market_chart_by_id(
            id="bitcoin",
            vs_currency="zar",
            days=days
        )
        return history['prices'][0][1] if history['prices'] else 0
    except Exception as e:
        print(f"⚠️ Bitcoin history error: {str(e)}")
        return 0

def fetch_market_data():
    """Main data fetching function"""
    cg = CoinGeckoAPI()
    now = datetime.now()
    
    try:
        # JSE All Share
        jse = yf.Ticker("^JN0U.JO").history(period="1d")["Close"].iloc[-1]
        
        # Forex Rates
        usdzar = yf.Ticker("USDZAR=X").history(period="1d")["Close"].iloc[-1]
        eurzar = yf.Ticker("EURZAR=X").history(period="1d")["Close"].iloc[-1]
        gbpzar = yf.Ticker("GBPZAR=X").history(period="1d")["Close"].iloc[-1]

        # Commodities
        brent = yf.Ticker("BZ=F").history(period="1d")["Close"].iloc[-1]
        gold = yf.Ticker("GC=F").history(period="1d")["Close"].iloc[-1]

        # S&P 500
        sp500 = yf.Ticker("^GSPC").history(period="1d")["Close"].iloc[-1]

        # Bitcoin Data
        bitcoin = cg.get_price(ids="bitcoin", vs_currencies="zar")["bitcoin"]["zar"]
        btc_1m = get_bitcoin_history(cg, 30)
        btc_ytd = get_bitcoin_history(cg, datetime.now().timetuple().tm_yday)

        return {
            "timestamp": now.strftime("%Y-%m-%d %H:%M"),
            "JSE All Share": {
                "Today": jse,
                "Change": calculate_percentage(fetch_historical_data("^JN0U.JO", 1), jse),
                "Monthly": calculate_percentage(fetch_historical_data("^JN0U.JO", 30), jse),
                "YTD": calculate_percentage(fetch_historical_data("^JN0U.JO", 365), jse)
            },
            # ... similar structures for other metrics ...
        }
    except Exception as e:
        print(f"❌ Data fetch failed: {str(e)}")
        return None
