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

def safe_yfinance_fetch(ticker, max_retries=3, delay=1):
    for attempt in range(max_retries):
        try:
            return ticker.history(period="7d", interval="1d")  # Increased from 5d to 7d
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            time.sleep(delay)
    return None

def fetch_market_data() -> Optional[Dict[str, Any]]:
    try:
        sa_tz = pytz.timezone("Africa/Johannesburg")
        now = datetime.now(sa_tz)
        today_str = now.strftime('%Y-%m-%d')

        # Increased buffer days for more reliable lookback
        one_day_ago = (now - timedelta(days=2)).strftime('%Y-%m-%d')  # 2-day window
        one_month_ago = (now - timedelta(days=40)).strftime('%Y-%m-%d')  # 40-day buffer
        ytd_start = datetime(now.year, 1, 1).strftime('%Y-%m-%d')

        tickers = {
            "JSEALSHARE": ["^J203.JO", "J203.JO"],  # Multiple ticker options
            "USDZAR": "USDZAR=X",
            "EURZAR": "EURZAR=X",
            "GBPZAR": "GBPZAR=X",
            "BRENT": "BZ=F",
            "GOLD": "GC=F",
            "SP500": "^GSPC"
        }

        data = {}

        # Modified JSE handling with fallback tickers
        jse_data = None
        for ticker in tickers["JSEALSHARE"]:
            try:
                tkr = yf.Ticker(ticker)
                hist = tkr.history(period="40d")  # Extended history
                if len(hist) < 5:  # Require minimum data
                    continue
                
                today_val = hist["Close"].iloc[-1]
                # Find most recent previous trading day
                day_ago_val = hist["Close"].iloc[-2] if len(hist) > 1 else today_val
                
                # Get exact monthly (22 trading days ≈ 1 month)
                month_ago_val = hist["Close"].iloc[-22] if len(hist) > 21 else today_val
                
                ytd_hist = tkr.history(start=ytd_start)
                ytd_val = ytd_hist["Close"].iloc[0] if not ytd_hist.empty else None

                jse_data = {
                    "Today": float(today_val),
                    "Change": calculate_percentage(day_ago_val, today_val),
                    "Monthly": calculate_percentage(month_ago_val, today_val),
                    "YTD": calculate_percentage(ytd_val, today_val) if ytd_val else 0.0
                }
                break
            except Exception:
                continue
        
        if jse_data:
            data["JSEALSHARE"] = jse_data
        else:
            print("⚠️ Could not fetch JSE data from any ticker")

        # Process other tickers normally
        for label, symbol in [(k,v) for k,v in tickers.items() if k != "JSEALSHARE"]:
            try:
                ticker = yf.Ticker(symbol)
                daily_hist = safe_yfinance_fetch(ticker)
                if daily_hist is None or daily_hist.empty:
                    continue

                today_val = daily_hist["Close"].iloc[-1]
                day_ago_val = daily_hist["Close"].iloc[-2] if len(daily_hist) > 1 else today_val
                
                monthly_hist = ticker.history(start=one_month_ago, end=one_day_ago)
                month_ago_val = monthly_hist["Close"].iloc[0] if not monthly_hist.empty else None
                
                ytd_hist = ticker.history(start=ytd_start)
                ytd_val = ytd_hist["Close"].iloc[0] if not ytd_hist.empty else None

                data[label] = {
                    "Today": float(today_val),
                    "Change": calculate_percentage(day_ago_val, today_val),
                    "Monthly": calculate_percentage(month_ago_val, today_val),
                    "YTD": calculate_percentage(ytd_val, today_val) if ytd_val else 0.0
                }

            except Exception as e:
                print(f"⚠️ Error fetching {label}: {str(e)}")
                continue

        # Improved Bitcoin handling
        try:
            cg = CoinGeckoAPI()
            btc_data = cg.get_coin_market_chart_range_by_id(
                'bitcoin', 'zar',
                from_timestamp=int((datetime.now() - timedelta(days=2)).timestamp()),
                to_timestamp=int(datetime.now().timestamp())
            )
            
            if not btc_data.get('prices'):
                raise ValueError("No BTC price data")
                
            prices = btc_data['prices']
            btc_today = prices[-1][1]
            
            # Find most recent prices with fallbacks
            btc_day_ago = next(
                (p[1] for p in prices 
                if (datetime.now() - datetime.fromtimestamp(p[0]/1000)).days == 1),
                prices[-2][1] if len(prices) > 1 else btc_today
            )
            
            data["BITCOINZAR"] = {
                "Today": float(btc_today),
                "Change": calculate_percentage(btc_day_ago, btc_today),
                "Monthly": 0.0,  # Simplified for now
                "YTD": 0.0      # Simplified for now
            }

        except Exception as e:
            print(f"⚠️ BTC fetch error: {str(e)}")
            data["BITCOINZAR"] = {
                "Today": 0.0,
                "Change": 0.0,
                "Monthly": 0.0,
                "YTD": 0.0
            }

        data["timestamp"] = now.strftime("%d %b %Y, %H:%M")
        return data

    except Exception as e:
        print(f"❌ Critical error: {str(e)}")
        return None
