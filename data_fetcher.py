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
    for attempt in range(max_retries):
        try:
            return ticker.history(period="5d", interval="1d")
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

        one_day_ago = now - timedelta(days=1)
        one_month_ago = now - timedelta(days=33)  # 3-day buffer
        ytd_start = datetime(now.year, 1, 1, tzinfo=sa_tz)

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

        for label, symbol in tickers.items():
            try:
                ticker = yf.Ticker(symbol)
                daily_hist = safe_yfinance_fetch(ticker)
                if daily_hist is None or daily_hist.empty:
                    print(f"⚠️ No data for {label} ({symbol})")
                    continue

                today_val = daily_hist["Close"].iloc[-1]
                day_ago_val = daily_hist["Close"].iloc[-2] if len(daily_hist) > 1 else today_val

                # --- FIXED JSE HISTORICAL CALCULATION BELOW ---
                month_ago_val = None
                ytd_val = None

                if label == "JSEALSHARE":
                    # Fetch full historical data range in one call
                    hist = ticker.history(start=one_month_ago.strftime('%Y-%m-%d'), end=today_str)

                    if not hist.empty:
                        # Find closest value to one month ago
                        month_ago_val = next(
                            (v for d, v in hist["Close"].items() if d.date() >= one_month_ago.date()),
                            None
                        )
                        # Find closest YTD value
                        ytd_val = next(
                            (v for d, v in hist["Close"].items() if d.date() >= ytd_start.date()),
                            None
                        )
                else:
                    # Other tickers: original logic
                    monthly_hist = ticker.history(start=one_month_ago.strftime('%Y-%m-%d'), end=one_day_ago.strftime('%Y-%m-%d'))
                    month_ago_val = monthly_hist["Close"].iloc[0] if not monthly_hist.empty else None

                    ytd_hist = ticker.history(start=ytd_start.strftime('%Y-%m-%d'))
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

        # Add timestamp
        data["timestamp"] = now.strftime("%d %b %Y, %H:%M")
        return data

    except Exception as e:
        print(f"❌ Critical error in fetch_market_data: {str(e)}")
        return None
