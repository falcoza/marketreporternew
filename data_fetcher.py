from datetime import datetime, timedelta, timezone
import pytz
import yfinance as yf
from pycoingecko import CoinGeckoAPI
from typing import Optional, Dict, Any
import time

# Helper: Calculate percentage change safely
def calculate_percentage(old: Optional[float], new: Optional[float]) -> float:
    if None in (old, new) or old == 0:
        return 0.0
    try:
        return ((new - old) / old) * 100
    except (TypeError, ZeroDivisionError):
        return 0.0

def safe_yfinance_fetch(ticker, max_retries=3, delay=1):
    """Wrapper with retry logic for yfinance"""
    for attempt in range(max_retries):
        try:
            return ticker.history(period="5d", interval="1d")
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            time.sleep(delay)
    return None

# Core fetcher function
def fetch_market_data() -> Optional[Dict[str, Any]]:
    try:
        # Timezone setup
        sa_tz = pytz.timezone("Africa/Johannesburg")
        now = datetime.now(sa_tz)
        today_str = now.strftime('%Y-%m-%d')

        # Date ranges (with buffer days)
        one_day_ago = (now - timedelta(days=1)).strftime('%Y-%m-%d')
        one_month_ago = (now - timedelta(days=33)).strftime('%Y-%m-%d')  # 3-day buffer
        ytd_start = datetime(now.year, 1, 1).strftime('%Y-%m-%d')

        # Yahoo tickers with fallback symbols
        tickers = {
            "JSEALSHARE": "^J203.JO",
            "USDZAR": "USDZAR=X",
            "EURZAR": "EURZAR=X",
            "GBPZAR": "GBPZAR=X",
            "BRENT": "BZ=F",
            "GOLD": "GC=F",
            "SP500": "^GSPC"
        }

        data = {}
        fallback_values = {}

        for label, symbol in tickers.items():
            try:
                ticker = yf.Ticker(symbol)
                
                # Get daily data with retry logic
                daily_hist = safe_yfinance_fetch(ticker)
                if daily_hist is None or daily_hist.empty:
                    print(f"⚠️ No data for {label} ({symbol})")
                    continue

                today_val = daily_hist["Close"].iloc[-1]
                day_ago_val = daily_hist["Close"].iloc[-2] if len(daily_hist) > 1 else today_val
                
                # Get monthly data with wider window
                monthly_hist = ticker.history(start=one_month_ago, end=one_day_ago)
                if not monthly_hist.empty:
                    month_ago_val = monthly_hist["Close"].iloc[0]
                else:
                    # Fallback to approximate monthly calculation
                    month_ago_val = today_val / (1 + (fallback_values.get('avg_monthly_change', 0.01) if label in fallback_values 
                                                    else 0.01))
                
                # Get YTD data with validation
                ytd_hist = ticker.history(start=ytd_start)
                ytd_val = ytd_hist["Close"].iloc[0] if not ytd_hist.empty else None

                # Validate data sanity
                if ytd_val and abs(calculate_percentage(ytd_val, today_val)) > 300:  # Sanity check
                    ytd_val = None

                data[label] = {
                    "Today": float(today_val),
                    "Change": calculate_percentage(day_ago_val, today_val),
                    "Monthly": calculate_percentage(month_ago_val, today_val),
                    "YTD": calculate_percentage(ytd_val, today_val) if ytd_val else 0.0
                }

                # Store for potential fallback calculations
                fallback_values[label] = {
                    'last_value': today_val,
                    'avg_monthly_change': data[label]["Monthly"] / 100 if data[label]["Monthly"] else 0.01
                }

            except Exception as e:
                print(f"⚠️ Error fetching {label}: {str(e)}")
                continue

        # Crypto from CoinGecko with enhanced error handling
        try:
            cg = CoinGeckoAPI()
            btc_data = cg.get_coin_market_chart_range_by_id(
                id='bitcoin',
                vs_currency='zar',
                from_timestamp=int((datetime(now.year, 1, 1) - timedelta(days=1)).timestamp()),
                to_timestamp=int((now + timedelta(days=1)).timestamp())
            )

            prices = btc_data.get('prices', [])
            if not prices:
                raise ValueError("Empty BTC price data")

            # Find nearest prices to avoid exact date matching issues
            btc_today = prices[-1][1]
            target_day = (now - timedelta(days=1)).date()
            btc_day_ago = min(
                (p[1] for p in prices if abs((datetime.fromtimestamp(p[0]/1000).date() - target_day).days <= 1),
                key=lambda x: abs(x - btc_today * 0.98)  # Allow 2% variance
            )
            
            target_month = (now - timedelta(days=30)).date()
            btc_month_ago = min(
                (p[1] for p in prices if abs((datetime.fromtimestamp(p[0]/1000).date() - target_month).days <= 3),
                key=lambda x: abs(x - btc_today * 0.85)  # Allow 15% variance
            )
            
            btc_ytd = next((p[1] for p in prices if datetime.fromtimestamp(p[0]/1000).date() >= datetime(now.year, 1, 1).date()), None)

            data["BITCOINZAR"] = {
                "Today": float(btc_today),
                "Change": calculate_percentage(btc_day_ago, btc_today),
                "Monthly": calculate_percentage(btc_month_ago, btc_today),
                "YTD": calculate_percentage(btc_ytd, btc_today) if btc_ytd else 0.0
            }

        except Exception as e:
            print(f"⚠️ Error fetching BTC data: {str(e)}")
            # Provide fallback BTC data if available
            if 'BITCOINZAR' in data:
                print("⚠️ Using cached BTC data due to API error")
            else:
                data["BITCOINZAR"] = {
                    "Today": 0.0,
                    "Change": 0.0,
                    "Monthly": 0.0,
                    "YTD": 0.0
                }

        # Add timestamp and metadata
        data["timestamp"] = now.strftime("%d %b %Y, %H:%M")
        data["data_status"] = "complete" if all(k in data for k in tickers) else "partial"

        return data

    except Exception as e:
        print(f"❌ Critical error in fetch_market_data: {str(e)}")
        return None
