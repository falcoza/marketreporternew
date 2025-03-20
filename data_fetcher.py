import yfinance as yf
from pycoingecko import CoinGeckoAPI
from datetime import datetime, timedelta

def calculate_percentage(old, new):
    if old is None or new is None or old == 0:
        return 0.0
    return ((new - old) / old) * 100

def fetch_historical(ticker, days):
    try:
        data = yf.Ticker(ticker).history(
            period=f"{days}d",
            interval="1d"
        )
        return data['Close'].iloc[0] if not data.empty else None
    except Exception as e:
        print(f"⚠️ History error {ticker}: {str(e)}")
        return None

def get_bitcoin_history(cg, days):
    try:
        history = cg.get_coin_market_chart_by_id(
            "bitcoin", "zar", days
        )['prices']
        return history[0][1] if len(history) > 0 else 0
    except Exception as e:
        print(f"⚠️ Bitcoin history error: {str(e)}")
        return 0

def fetch_market_data():
    cg = CoinGeckoAPI()
    now = datetime.now()
    
    try:
        # Equity indices
        jse = yf.Ticker("^JN0U.JO").history(period="1d")["Close"].iloc[-1]
        
        # Forex pairs
        usdzar = yf.Ticker("USDZAR=X").history(period="1d")["Close"].iloc[-1]
        eurzar = yf.Ticker("EURZAR=X").history(period="1d")["Close"].iloc[-1]
        gbpzar = yf.Ticker("GBPZAR=X").history(period="1d")["Close"].iloc[-1]

        # Commodities
        brent = yf.Ticker("BZ=F").history(period="1d")["Close"].iloc[-1]
        gold = yf.Ticker("GC=F").history(period="1d")["Close"].iloc[-1]

        # US Equity
        sp500 = yf.Ticker("^GSPC").history(period="1d")["Close"].iloc[-1]

        # Cryptocurrency
        bitcoin = cg.get_price("bitcoin", "zar")["bitcoin"]["zar"]
        btc_1m = get_bitcoin_history(cg, 30)
        btc_ytd = get_bitcoin_history(cg, datetime.now().timetuple().tm_yday)

        return {
            "timestamp": now.strftime("%Y-%m-%d %H:%M"),
            "JSEALSHARE": {
                "Today": jse,
                "Change": calculate_percentage(fetch_historical("^JN0U.JO", 1), jse),
                "Monthly": calculate_percentage(fetch_historical("^JN0U.JO", 30), jse),
                "YTD": calculate_percentage(fetch_historical("^JN0U.JO", 365), jse)
            },
            "USDZAR": {
                "Today": usdzar,
                "Change": calculate_percentage(fetch_historical("USDZAR=X", 1), usdzar),
                "Monthly": calculate_percentage(fetch_historical("USDZAR=X", 30), usdzar),
                "YTD": calculate_percentage(fetch_historical("USDZAR=X", 365), usdzar)
            },
            "EURZAR": {
                "Today": eurzar,
                "Change": calculate_percentage(fetch_historical("EURZAR=X", 1), eurzar),
                "Monthly": calculate_percentage(fetch_historical("EURZAR=X", 30), eurzar),
                "YTD": calculate_percentage(fetch_historical("EURZAR=X", 365), eurzar)
            },
            "GBPZAR": {
                "Today": gbpzar,
                "Change": calculate_percentage(fetch_historical("GBPZAR=X", 1), gbpzar),
                "Monthly": calculate_percentage(fetch_historical("GBPZAR=X", 30), gbpzar),
                "YTD": calculate_percentage(fetch_historical("GBPZAR=X", 365), gbpzar)
            },
            "BRENT": {
                "Today": brent,
                "Change": calculate_percentage(fetch_historical("BZ=F", 1), brent),
                "Monthly": calculate_percentage(fetch_historical("BZ=F", 30), brent),
                "YTD": calculate_percentage(fetch_historical("BZ=F", 365), brent)
            },
            "GOLD": {
                "Today": gold,
                "Change": calculate_percentage(fetch_historical("GC=F", 1), gold),
                "Monthly": calculate_percentage(fetch_historical("GC=F", 30), gold),
                "YTD": calculate_percentage(fetch_historical("GC=F", 365), gold)
            },
            "SP500": {
                "Today": sp500,
                "Change": calculate_percentage(fetch_historical("^GSPC", 1), sp500),
                "Monthly": calculate_percentage(fetch_historical("^GSPC", 30), sp500),
                "YTD": calculate_percentage(fetch_historical("^GSPC", 365), sp500)
            },
            "BITCOINZAR": {
                "Today": bitcoin,
                "Change": calculate_percentage(btc_1m, bitcoin),
                "Monthly": calculate_percentage(btc_1m, bitcoin),
                "YTD": calculate_percentage(btc_ytd, bitcoin)
            }
        }
    except Exception as e:
        print(f"❌ Data fetch failed: {str(e)}")
        return None
