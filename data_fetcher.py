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
    for ticker in ["^J203.JO", "J203.JO"]:  # Try both common tickers
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period="60d")  # Extended period for reliability
            
            if len(hist) < 5:  # Need at least 5 trading days
                continue
                
            current = hist["Close"].iloc[-1]
            
            # Find most recent trading day (skip weekends/holidays)
            prev_day = hist["Close"].iloc[-2] if len(hist) > 1 else current
            
            # Get 30 trading days ago (≈1 month)
            month_ago = hist["Close"].iloc[-22] if len(hist) > 21 else current
            
            # Get YTD price
            ytd_hist = stock.history(start=f"{datetime.now().year}-01-01")
            ytd_price = ytd_hist["Close"].iloc[0] if not ytd_hist.empty else None
            
            return {
                "Today": round(current, 2),
                "Change": round(calculate_percentage(prev_day, current), 1),
                "Monthly": round(calculate_percentage(month_ago, current), 1),
                "YTD": round(calculate_percentage(ytd_price, current), 1) if ytd_price else 0.0
            }
        except Exception as e:
            print(f"JSE {ticker} error: {str(e)}")
            continue
    
    return {"Today": 0.0, "Change": 0.0, "Monthly": 0.0, "YTD": 0.0}

def get_gold_in_zar(usd_zar_rate: float) -> Dict[str, float]:
    """Convert gold price from USD to ZAR"""
    try:
        gold_usd = yf.Ticker("GC=F").history(period="10d")
        if gold_usd.empty:
            return {"Today": 0.0, "Change": 0.0, "Monthly": 0.0, "YTD": 0.0}
            
        current_usd = gold_usd["Close"].iloc[-1]
        current_zar = current_usd * usd_zar_rate
        
        # Get previous day
        prev_day_usd = gold_usd["Close"].iloc[-2] if len(gold_usd) > 1 else current_usd
        prev_day_zar = prev_day_usd * usd_zar_rate
        
        # Get monthly (simplified)
        monthly_usd = gold_usd["Close"].iloc[-22] if len(gold_usd) > 21 else current_usd
        monthly_zar = monthly_usd * usd_zar_rate
        
        return {
            "Today": round(current_zar, 2),
            "Change": round(calculate_percentage(prev_day_zar, current_zar), 1),
            "Monthly": round(calculate_percentage(monthly_zar, current_zar), 1),
            "YTD": 0.0  # Will be calculated separately
        }
    except Exception as e:
        print(f"Gold conversion error: {str(e)}")
        return {"Today": 0.0, "Change": 0.0, "Monthly": 0.0, "YTD": 0.0}

def get_bitcoin_data() -> Dict[str, float]:
    """Improved Bitcoin data with proper percentage changes"""
    try:
        cg = CoinGeckoAPI()
        
        # Get current price
        btc_now = cg.get_price(ids="bitcoin", vs_currencies="zar")["bitcoin"]["zar"]
        
        # Get historical data (past 30 days)
        btc_hist = cg.get_coin_market_chart_by_id(
            id="bitcoin",
            vs_currency="zar",
            days="30"
        )["prices"]
        
        if not btc_hist:
            return {"Today": 0.0, "Change": 0.0, "Monthly": 0.0, "YTD": 0.0}
        
        # Find yesterday's price (24h ago)
        now = datetime.now()
        yesterday = [p[1] for p in btc_hist 
                    if (now - datetime.fromtimestamp(p[0]/1000)).days == 1]
        btc_yesterday = yesterday[-1] if yesterday else btc_hist[-2][1]
        
        # Get monthly price (30 days ago)
        monthly = [p[1] for p in btc_hist 
                  if (now - datetime.fromtimestamp(p[0]/1000)).days >= 28]  # 28-31 day range
        btc_monthly = monthly[0] if monthly else btc_hist[0][1]
        
        # Get YTD price
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
    except Exception as e:
        print(f"Bitcoin error: {str(e)}")
        return {"Today": 0.0, "Change": 0.0, "Monthly": 0.0, "YTD": 0.0}

def fetch_market_data() -> Optional[Dict[str, Any]]:
    try:
        sa_tz = pytz.timezone("Africa/Johannesburg")
        now = datetime.now(sa_tz)
        data = {}
        
        # 1. Get USD/ZAR rate first (needed for gold conversion)
        zar_rate = yf.Ticker("ZAR=X").history(period="2d")["Close"].iloc[-1]
        
        # 2. Get all market data
        data["JSEALSHARE"] = get_jse_data()
        data["USDZAR"] = {
            "Today": round(zar_rate, 2),
            "Change": 0.0,  # Will calculate later
            "Monthly": 0.0,
            "YTD": 0.0
        }
        data["GOLD"] = get_gold_in_zar(zar_rate)
        data["BITCOINZAR"] = get_bitcoin_data()
        
        # 3. Get other forex pairs
        for pair in ["EURZAR=X", "GBPZAR=X"]:
            try:
                hist = yf.Ticker(pair).history(period="10d")
                if hist.empty:
                    continue
                    
                current = hist["Close"].iloc[-1]
                prev = hist["Close"].iloc[-2] if len(hist) > 1 else current
                
                data[pair.replace("=X", "")] = {
                    "Today": round(current, 2),
                    "Change": round(calculate_percentage(prev, current), 1),
                    "Monthly": 0.0,
                    "YTD": 0.0
                }
            except Exception:
                continue
        
        # 4. Add timestamp
        data["timestamp"] = now.strftime("%d %b %Y, %H:%M")
        
        return data
        
    except Exception as e:
        print(f"Critical error: {str(e)}")
        return None
