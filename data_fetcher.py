from datetime import datetime, timedelta, timezone
import pytz
import yfinance as yf
from pycoingecko import CoinGeckoAPI
from typing import Optional, Dict, Any
import time
import pandas as pd

def calculate_percentage(old: Optional[float], new: Optional[float]) -> float:
    if None in (old, new) or old == 0:
        return 0.0
    try:
        return ((new - old) / old) * 100
    except (TypeError, ZeroDivisionError):
        return 0.0

def get_jse_data() -> Dict[str, float]:
    """Accurate JSE All Share calculations with proper date alignment"""
    for ticker in ["^J203.JO", "J203.JO"]:
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period="1y", interval="1d")
            
            if len(hist) < 5:
                continue
                
            current = hist["Close"].iloc[-1]
            current_date = hist.index[-1].date()
            
            # 1-day change (exact previous trading day)
            prev_trading_days = hist[hist.index.date < current_date]
            if len(prev_trading_days) > 0:
                prev_day = prev_trading_days["Close"].iloc[-1]
                day_change = calculate_percentage(prev_day, current)
            else:
                day_change = 0.0
            
            # Monthly change (22 trading days ago)
            month_ago = hist["Close"].iloc[-22] if len(hist) > 21 else current
            month_change = calculate_percentage(month_ago, current)
            
            # YTD change (first trading day of year)
            ytd_data = hist[hist.index.year == current_date.year]
            if len(ytd_data) > 0:
                ytd_price = ytd_data["Close"].iloc[0]
                ytd_change = calculate_percentage(ytd_price, current)
            else:
                ytd_change = 0.0
            
            return {
                "Today": round(current, 2),
                "Change": round(day_change, 1),
                "Monthly": round(month_change, 1),
                "YTD": round(ytd_change, 1)
            }
        except Exception as e:
            print(f"JSE error ({ticker}): {str(e)}")
            continue
    
    return {"Today": 0.0, "Change": 0.0, "Monthly": 0.0, "YTD": 0.0}

def get_gold_in_zar(usd_zar_rate: float) -> Dict[str, float]:
    """Gold prices properly converted to ZAR"""
    try:
        gold = yf.Ticker("GC=F")
        hist = gold.history(period="90d", interval="1d")
        
        if hist.empty:
            return {"Today": 0.0, "Change": 0.0, "Monthly": 0.0, "YTD": 0.0}
            
        current_usd = hist["Close"].iloc[-1]
        current_zar = current_usd * usd_zar_rate
        
        # Previous trading day
        prev_day = hist["Close"].iloc[-2] if len(hist) > 1 else current_usd
        prev_day_zar = prev_day * usd_zar_rate
        
        # Monthly change
        month_ago = hist["Close"].iloc[-22] if len(hist) > 21 else current_usd
        month_ago_zar = month_ago * usd_zar_rate
        
        # YTD price
        ytd_hist = gold.history(start=f"{datetime.now().year}-01-01", interval="1d")
        ytd_price = ytd_hist["Close"].iloc[0] * usd_zar_rate if not ytd_hist.empty else None
        
        return {
            "Today": round(current_zar, 2),
            "Change": round(calculate_percentage(prev_day_zar, current_zar), 1),
            "Monthly": round(calculate_percentage(month_ago_zar, current_zar), 1),
            "YTD": round(calculate_percentage(ytd_price, current_zar), 1) if ytd_price else 0.0
        }
    except Exception as e:
        print(f"Gold error: {str(e)}")
        return {"Today": 0.0, "Change": 0.0, "Monthly": 0.0, "YTD": 0.0}

def fetch_market_data() -> Optional[Dict[str, Any]]:
    try:
        sa_tz = pytz.timezone("Africa/Johannesburg")
        now = datetime.now(sa_tz)
        data = {}
        
        # 1. Get USD/ZAR rate first
        usd_zar = yf.Ticker("ZAR=X").history(period="10d")
        zar_rate = usd_zar["Close"].iloc[-1] if not usd_zar.empty else 0.0
        
        # 2. Get all market data
        data["JSEALSHARE"] = get_jse_data()
        data["USDZAR"] = {
            "Today": round(zar_rate, 2),
            "Change": round(calculate_percentage(usd_zar["Close"].iloc[-2], zar_rate), 1) if len(usd_zar) > 1 else 0.0,
            "Monthly": 0.0,  # Will be calculated in next version
            "YTD": 0.0       # Will be calculated in next version
        }
        data["GOLD"] = get_gold_in_zar(zar_rate)
        
        # 3. Get Bitcoin data
        try:
            cg = CoinGeckoAPI()
            btc_data = cg.get_coin_market_chart_by_id("bitcoin", "zar", days="30")
            prices = btc_data["prices"]
            
            current = prices[-1][1]
            yesterday = next((p[1] for p in prices if 
                            (datetime.fromtimestamp(prices[-1][0]/1000) - 
                             datetime.fromtimestamp(p[0]/1000)).days == 1), prices[-2][1])
            
            data["BITCOINZAR"] = {
                "Today": round(current, 2),
                "Change": round(calculate_percentage(yesterday, current), 1),
                "Monthly": round(calculate_percentage(prices[0][1], current), 1),
                "YTD": 0.0  # Will be calculated in next version
            }
        except Exception as e:
            print(f"Bitcoin error: {str(e)}")
            data["BITCOINZAR"] = {"Today": 0.0, "Change": 0.0, "Monthly": 0.0, "YTD": 0.0}
        
        data["timestamp"] = now.strftime("%d %b %Y, %H:%M")
        return data
        
    except Exception as e:
        print(f"Critical error: {str(e)}")
        return None
