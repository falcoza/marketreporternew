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

                # Use a wider 5-day window to ensure two valid points
                hist = ticker.history(period="5d")

                if hist.empty:
                    print(f"⚠️ No data for {label} ({symbol})")
                    continue

                closes = hist["Close"].dropna()

                if len(closes) < 2:
                    print(f"⚠️ Not enough data points for {label}")
                    continue

                today_val = closes[-1]
                prev_val = closes[-2]

                # For monthly and YTD, use explicit date-based history
                long_hist = ticker.history(start=ytd_start, end=today_str)
                long_closes = long_hist["Close"].dropna()
                month_ago_val = long_closes.loc[one_month_ago] if one_month_ago in long_closes.index.strftime('%Y-%m-%d') else None
                ytd_val = long_closes.iloc[0] if not long_closes.empty else None

                data[label] = {
                    "Today": float(today_val),
                    "Change": calculate_percentage(prev_val, today_val),
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
