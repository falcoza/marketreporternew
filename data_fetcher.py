import yfinance as yf
from pycoingecko import CoinGeckoAPI
from datetime import datetime, timedelta

# Helper functions first to avoid NameError
def calculate_percentage(old, new):
    """Safe percentage calculation with zero handling"""
    if old is None or new is None or old == 0:
        return 0.0
    return ((new - old) / old) * 100

def fetch_historical_data(ticker, days=1):
    """Robust historical data fetcher"""
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
    """Main data fetcher with proper error handling"""
    cg = CoinGeckoAPI()  # Initialize here for Bitcoin data
    now = datetime.now()
    
    try:
        # JSE All Share
        jse = yf.Ticker("^JN0U.JO").history(period="1d")["Close"].iloc[-1]
        jse_prev = fetch_historical_data("^JN0U.JO", 1)
        
        # Forex Rates
        usdzar = yf.Ticker("USDZAR=X").history(period="1d")["Close"].iloc[-1]
        eurzar = yf.Ticker("EURZAR=X").history(period="1d")["Close"].iloc[-1]
        gbpzar = yf.Ticker("GBPZAR=X").history(period="1d")["Close"].iloc[-1]

        # Commodities
        brent = yf.Ticker("BZ=F").history(period="1d")["Close"].iloc[-1]
        gold = yf.Ticker("GC=F").history(period="1d")["Close"].iloc[-1]

        # S&P 500
        sp500 = yf.Ticker("^GSPC").history(period="1d")["Close"].iloc[-1]

        # Bitcoin with CoinGecko
        try:
            bitcoin = cg.get_price(ids="bitcoin", vs_currencies="zar")["bitcoin"]["zar"]
            btc_month = get_bitcoin_history(cg, 30)
            btc_ytd = get_bitcoin_ytd_start_price(cg)
        except Exception as e:
            print(f"⚠️ Bitcoin data error: {str(e)}")
            bitcoin = 0
            btc_month = 0
            btc_ytd = 0

        return {
            "timestamp": now.strftime("%Y-%m-%d %H:%M"),
            "JSE All Share": {
                "Today": jse,
                "Change": calculate_percentage(jse_prev, jse),
                "Monthly": calculate_percentage(fetch_historical_data("^JN0U.JO", 30), jse),
                "YTD": calculate_percentage(fetch_historical_data("^JN0U.JO", 365), jse)
            },
            "Rand/Dollar": {
                "Today": usdzar,
                "Change": calculate_percentage(fetch_historical_data("USDZAR=X", 1), usdzar),
                "Monthly": calculate_percentage(fetch_historical_data("USDZAR=X", 30), usdzar),
                "YTD": calculate_percentage(fetch_historical_data("USDZAR=X", 365), usdzar)
            },
            "Rand/Euro": {
                "Today": eurzar,
                "Change": calculate_percentage(fetch_historical_data("EURZAR=X", 1), eurzar),
                "Monthly": calculate_percentage(fetch_historical_data("EURZAR=X", 30), eurzar),
                "YTD": calculate_percentage(fetch_historical_data("EURZAR=X", 365), eurzar)
            },
            "Rand/GBP": {
                "Today": gbpzar,
                "Change": calculate_percentage(fetch_historical_data("GBPZAR=X", 1), gbpzar),
                "Monthly": calculate_percentage(fetch_historical_data("GBPZAR=X", 30), gbpzar),
                "YTD": calculate_percentage(fetch_historical_data("GBPZAR=X", 365), gbpzar)
            },
            "Brent ($/barrel)": {
                "Today": brent,
                "Change": calculate_percentage(fetch_historical_data("BZ=F", 1), brent),
                "Monthly": calculate_percentage(fetch_historical_data("BZ=F", 30), brent),
                "YTD": calculate_percentage(fetch_historical_data("BZ=F", 365), brent)
            },
            "Gold ($/oz)": {
                "Today": gold,
                "Change": calculate_percentage(fetch_historical_data("GC=F", 1), gold),
                "Monthly": calculate_percentage(fetch_historical_data("GC=F", 30), gold),
                "YTD": calculate_percentage(fetch_historical_data("GC=F", 365), gold)
            },
            "S&P500": {
                "Today": sp500,
                "Change": calculate_percentage(fetch_historical_data("^GSPC", 1), sp500),
                "Monthly": calculate_percentage(fetch_historical_data("^GSPC", 30), sp500),
                "YTD": calculate_percentage(fetch_historical_data("^GSPC", 365), sp500)
            },
            "Bitcoin (ZAR)": {
                "Today": bitcoin,
                "Change": calculate_percentage(btc_month, bitcoin),
                "Monthly": calculate_percentage(btc_month, bitcoin),
                "YTD": calculate_percentage(btc_ytd, bitcoin)
            }
        }
        
    except Exception as e:
        print(f"❌ Critical error in fetch_market_data: {str(e)}")
        return None

def get_bitcoin_history(cg, days):
    """Get Bitcoin historical price using existing CoinGecko client"""
    try:
        history = cg.get_coin_market_chart_by_id(
            id="bitcoin",
            vs_currency="zar",
            days=days
        )
        return history['prices'][0][1] if len(history['prices']) > 0 else 0
    except Exception as e:
        print(f"⚠️ Bitcoin history error: {str(e)}")
        return 0

def get_bitcoin_ytd_start_price(cg):
    """Get Bitcoin price from January 1st using existing client"""
    try:
        year_start = datetime(datetime.now().year, 1, 1).timestamp()
        history = cg.get_coin_market_chart_range_by_id(
            id="bitcoin",
            vs_currency="zar",
            from_timestamp=int(year_start),
            to_timestamp=int(year_start) + 86400
        )
        return history["prices"][0][1] if history["prices"] else 0
    except Exception as e:
        print(f"⚠️ Bitcoin YTD error: {str(e)}")
        return 0
