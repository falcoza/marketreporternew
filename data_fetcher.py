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

        one_day_ago = (now - timedelta(days=1)).strftime('%Y-%m-%d')
        one_month_ago = (now - timedelta(days=33)).strftime('%Y-%m-%d')
        ytd_start = datetime(now.year, 1, 1).strftime('%Y-%m-%d')

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

                # Fallback if previous day not available
                day_ago_val = next(
                    (val for val in reversed(daily_hist["Close"].values[:-1]) if val != today_val),
                    today_val
                )

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

        # Crypto from CoinGecko
        try:
            cg = CoinGeckoAPI()
            btc_data = cg.get_coin_market_chart_range_by_id(
                id='bitcoin',
                vs_currency='zar',
                from_timestamp=int(datetime.strptime(ytd_start, "%Y-%m-%d").timestamp()),
                to_timestamp=int(now.timestamp())
            )

            prices = btc_data.get('prices', [])
            if not prices:
                raise ValueError("Empty BTC price data")

            btc_today = prices[-1][1]

            def get_price_by_day(delta_days: int):
                target_date = (now - timedelta(days=delta_days)).date()
                for p in reversed(prices):
                    if datetime.fromtimestamp(p[0]/1000).date() <= target_date:
                        return p[1]
                return prices[0][1]

            btc_day_ago = get_price_by_day(1)
            btc_month_ago = get_price_by_day(30)
            btc_ytd = prices[0][1]

            data["BITCOINZAR"] = {
                "Today": float(btc_today),
                "Change": calculate_percentage(btc_day_ago, btc_today),
                "Monthly": calculate_percentage(btc_month_ago, btc_today),
                "YTD": calculate_percentage(btc_ytd, btc_today)
            }

        except Exception as e:
            print(f"⚠️ Error fetching BTC data: {str(e)}")
            data["BITCOINZAR"] = {
                "Today": 0.0,
                "Change": 0.0,
                "Monthly": 0.0,
                "YTD": 0.0
            }

        data["timestamp"] = now.strftime("%d %b %Y, %H:%M")
        return data

    except Exception as e:
        print(f"❌ Critical error in fetch_market_data: {str(e)}")
        return None
