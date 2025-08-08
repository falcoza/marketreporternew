from datetime import datetime, timedelta, timezone
import pytz
import yfinance as yf
from pycoingecko import CoinGeckoAPI
from typing import Optional, Dict, Any
import time

def calculate_percentage(old: Optional[float], new: Optional[float]) -> float:
    if None in (old, new) or old == 0:
        return 0.0
    try:
        return ((new - old) / old) * 100
    except (TypeError, ZeroDivisionError):
        return 0.0

def get_jse_data() -> Dict[str, float]:
    """Special handling for JSE All Share index"""
    for ticker in ["^J203.JO", "J203.JO"]:
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period="60d")
            
            if len(hist) < 5:
                continue
                
            current = hist["Close"].iloc[-1]
            prev_day = hist["Close"].iloc[-2] if len(hist) > 1 else current
            month_ago = hist["Close"].iloc[-22] if len(hist) > 21 else current
            
            ytd_hist = stock.history(start=f"{datetime.now().year}-01-01")
            ytd_price = ytd_hist["Close"].iloc[0] if not ytd_hist.empty else None
            
            return {
                "Today": round(current, 2),
                "Change": round(calculate_percentage(prev_day, current), 1),
                "Monthly": round(calculate_percentage(month_ago, current), 1),
                "YTD": round(calculate_percentage(ytd_price, current), 1) if ytd_price else 0.0
            }
        except Exception:
            continue
    
    return {"Today": 0.0, "Change": 0.0, "Monthly": 0.0, "YTD": 0.0}

def get_commodity_data(ticker: str) -> Dict[str, float]:
    """Get commodity data (Brent, Gold)"""
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period="30d")
        
        if hist.empty:
            return {"Today": 0.0, "Change": 0.0, "Monthly": 0.0, "YTD": 0.0}
            
        current = hist["Close"].iloc[-1]
        prev_day = hist["Close"].iloc[-2] if len(hist) > 1 else current
        month_ago = hist["Close"].iloc[-22] if len(hist) > 21 else current
        
        ytd_hist = stock.history(start=f"{datetime.now().year}-01-01")
        ytd_price = ytd_hist["Close"].iloc[0] if not ytd_hist.empty else None
        
        return {
            "Today": round(current, 2),
            "Change": round(calculate_percentage(prev_day, current), 1),
            "Monthly": round(calculate_percentage(month_ago, current), 1),
            "YTD": round(calculate_percentage(ytd_price, current), 1) if ytd_price else 0.0
        }
    except Exception:
        return {"Today": 0.0, "Change": 0.0, "Monthly": 0.0, "YTD": 0.0}

def get_bitcoin_data() -> Dict[str, float]:
    """Get Bitcoin data in ZAR"""
    try:
        cg = CoinGeckoAPI()
        btc_now = cg.get_price(ids="bitcoin", vs_currencies="zar")["bitcoin"]["zar"]
        btc_hist = cg.get_coin_market_chart_by_id(id="bitcoin", vs_currency="zar", days="30")["prices"]
        
        if not btc_hist:
            return {"Today": 0.0, "Change": 0.0, "Monthly": 0.0, "YTD": 0.0}
        
        now = datetime.now()
        yesterday = [p[1] for p in btc_hist 
                    if (now - datetime.fromtimestamp(p[0]/1000)).days == 1]
        btc_yesterday = yesterday[-1] if yesterday else btc_hist[-2][1]
        
        monthly = [p[1] for p in btc_hist 
                  if (now - datetime.fromtimestamp(p[0]/1000)).days >= 28]
        btc_monthly = monthly[0] if monthly else btc_hist[0][1]
        
        ytd_data = cg.get_coin_market_chart_range_by_id(
            id="bitcoin",
            vs_currency="zar",
            from_timestamp=int(datetime(now.year, 1, 1).timestamp()),
            to_timestamp=int(now.timestamp())
        )
        btc_ytd = ytd_data["prices"][0][1] if ytd_data.get("prices") else None
        
        return {
            "Today": round(btc_now, 2),
            "Change": round(calculate_percentage(btc_yesterday, btc_now), 1),
            "Monthly": round(calculate_percentage(btc_monthly, btc_now), 1),
            "YTD": round(calculate_percentage(btc_ytd, btc_now), 1) if btc_ytd else 0.0
        }
    except Exception:
        return {"Today": 0.0, "Change": 0.0, "Monthly": 0.0, "YTD": 0.0}

def fetch_market_data() -> Optional[Dict[str, Any]]:
    try:
        sa_tz = pytz.timezone("Africa/Johannesburg")
        now = datetime.now(sa_tz)
        data = {}
        
        # Get all required market data
        data["JSEALSHARE"] = get_jse_data()
        data["BRENT"] = get_commodity_data("BZ=F")
        data["GOLD"] = get_commodity_data("GC=F")
        data["SP500"] = get_commodity_data("^GSPC")
        data["BITCOINZAR"] = get_bitcoin_data()
        
        # Get forex rates
        for pair in ["ZAR=X", "EURZAR=X", "GBPZAR=X"]:
            try:
                hist = yf.Ticker(pair).history(period="10d")
                if hist.empty:
                    continue
                    
                current = hist["Close"].iloc[-1]
                prev = hist["Close"].iloc[-2] if len(hist) > 1 else current
                
                key = "USDZAR" if pair == "ZAR=X" else pair.replace("=X", "")
                data[key] = {
                    "Today": round(current, 2),
                    "Change": round(calculate_percentage(prev, current), 1),
                    "Monthly": 0.0,  # Simplified for now
                    "YTD": 0.0       # Simplified for now
                }
            except Exception:
                continue
        
        data["timestamp"] = now.strftime("%d %b %Y, %H:%M")
        return data
        
    except Exception as e:
        print(f"Critical error: {str(e)}")
        return None
