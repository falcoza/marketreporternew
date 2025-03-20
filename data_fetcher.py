import yfinance as yf
from pycoingecko import CoinGeckoAPI
from datetime import datetime, timedelta
import pandas as pd

def calculate_percentage(old, new):
    return ((new - old) / old) * 100 if old != 0 else 0

def fetch_historical_data(ticker, days=1):
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    data = yf.Ticker(ticker).history(start=start_date, end=end_date, interval="1d")
    return data['Close'].iloc[0] if not data.empty else 0

def fetch_market_data():
    cg = CoinGeckoAPI()
    now = datetime.now()
    
    # Current prices
    jse = yf.Ticker("^JN0U.JO").history(period="1d")["Close"].iloc[-1]
    usdzar = yf.Ticker("USDZAR=X").history(period="1d")["Close"].iloc[-1]
    
    # Historical data
    jse_prev = fetch_historical_data("^JN0U.JO", 2)
    usdzar_month = fetch_historical_data("USDZAR=X", 30)
    
    return {
        "timestamp": now.strftime("%Y-%m-%d %H:%M"),
        "JSE All Share": {
            "Today": jse,
            "Change": calculate_percentage(jse_prev, jse),
            "Monthly": calculate_percentage(fetch_historical_data("^JN0U.JO", 30), jse),
            "YTD": calculate_percentage(fetch_historical_data("^JN0U.JO", 365), jse)
        },
        # Add similar structures for other metrics
        # ...
    }
