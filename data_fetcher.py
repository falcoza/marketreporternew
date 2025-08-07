from datetime import datetime, timedelta, timezone
import pytz
import yfinance as yf
from pycoingecko import CoinGeckoAPI
from typing import Optional, Dict, Any

# Helper: Calculate percentage change safely
def calculate_percentage(old: Optional[float], new: Optional[float]) -> float:
    if None in (old, new) or old == 0:
        return 0.0
    try:
        return ((new - old) / old) * 100
    except (TypeError, ZeroDivisionError):
        return 0.0

# Core fetcher function
def fetch_market_data() -> Optional[Dict[str, Any]]:
    try:
        # Timezone setup
        sa_tz = pytz.timezone("Africa/Johannesburg")
        now = datetime.now(sa_tz)
        today_str = now.strftime('%Y-%m-%d')

        # Date ranges
        one_day_ago = (now - timedelta(days=1)).strftime('%Y-%m-%d')
        one_month_ago = (now - timedelta(days=30)).strftime('%Y-%m-%d')
        ytd_start = datetime(now.year, 1, 1).strftime('%Y-%m-%d')

        # Yahoo tickers
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
                hist = ticker.history(start=ytd_start, end=now.strftime('%Y-%m-%d'))

                if hist.empty:
                    print(f"⚠️ No data for {label} ({symbol})")
                    continue

                today_val = hist["Close"][-1]
                day_ago_val = ticker.history(period="2d", interval="1d")["Close"].iloc[0]  # Force daily bars
                month_ago_val = hist["Close"].loc[one_month_ago] if one_month_ago in hist["Close"] else None
                ytd_val = hist["Close"].iloc[0]

                data[label] = {
                    "Today": float(today_val),
                    "Change": calculate_percentage(day_ago_val, today_val),
                    "Monthly": calculate_percentage(month_ago_val, today_val),
                    "YTD": calculate_percentage(ytd_val, today_val)
                }
            except Exception as e:
                print(f"⚠️ Error fetching {label}: {e}")
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

            prices = btc_data['prices']
            if not prices:
                raise ValueError("Empty BTC price data")

            btc_today = prices[-1][1]
            btc_day_ago = next((p[1] for p in prices if datetime.fromtimestamp(p[0] / 1000, sa_tz).date() == (now - timedelta(days=1)).date()), None)
            btc_month_ago = next((p[1] for p in prices if datetime.fromtimestamp(p[0] / 1000, sa_tz).date() == (now - timedelta(days=30)).date()), None)
            btc_ytd = prices[0][1]

            data["BITCOINZAR"] = {
                "Today": float(btc_today),
                "Change": calculate_percentage(btc_day_ago, btc_today),
                "Monthly": calculate_percentage(btc_month_ago, btc_today),
                "YTD": calculate_percentage(btc_ytd, btc_today)
            }

        except Exception as e:
            print(f"⚠️ Error fetching BTC data: {e}")

        # Add timestamp
        data["timestamp"] = now.strftime("%d %b %Y, %H:%M")

        return data

    except Exception as e:
        print(f"❌ Critical error in fetch_market_data: {str(e)}")
        return None
