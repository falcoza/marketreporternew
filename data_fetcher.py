from pycoingecko import CoinGeckoAPI
import yfinance as yf
from datetime import datetime
from config import ASSETS

def fetch_market_data():
    """Retrieve live market data from various sources"""
    data = {"Date": datetime.now().strftime("%Y-%m-%d %H:%M")}
    
    try:
        # JSE All Share
        jse = yf.Ticker(ASSETS["jse"])
        data["JSE All Share"] = jse.history(period="1d")["Close"].iloc[-1]

        # Forex Rates
        forex_pairs = ["USDZAR=X", "EURZAR=X", "GBPZAR=X"]
        data.update({
            "Rand/Dollar": yf.Ticker(forex_pairs[0]).history(period="1d")["Close"].iloc[-1],
            "Rand/Euro": yf.Ticker(forex_pairs[1]).history(period="1d")["Close"].iloc[-1],
            "Rand/GBP": yf.Ticker(forex_pairs[2]).history(period="1d")["Close"].iloc[-1],
        })

        # Commodities
        data.update({
            "Brent ($/barrel)": yf.Ticker(ASSETS["commodities"][0]).history(period="1d")["Close"].iloc[-1],
            "Gold ($/oz)": yf.Ticker(ASSETS["commodities"][1]).history(period="1d")["Close"].iloc[-1]
        })

        # S&P 500
        data["S&P 500"] = yf.Ticker(ASSETS["sp500"]).history(period="1d")["Close"].iloc[-1]

        # Bitcoin (CoinGecko)
        cg = CoinGeckoAPI()
        data["Bitcoin (ZAR)"] = cg.get_price(ids=ASSETS["crypto"], vs_currencies="zar")["bitcoin"]["zar"]

        return data

    except Exception as e:
        print(f"Data fetch error: {e}")
        return None
