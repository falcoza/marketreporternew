from datetime import datetime, timedelta
from typing import Optional, Dict, Any
import time
import pytz
import yfinance as yf
from pycoingecko import CoinGeckoAPI  # kept for your BTC section if you use it

SAST = pytz.timezone("Africa/Johannesburg")

# ---------- helpers ----------
def calculate_percentage(old: Optional[float], new: Optional[float]) -> float:
    if old in (None, 0) or new is None:
        return 0.0
    try:
        return ((new - old) / old) * 100.0
    except Exception:
        return 0.0

def safe_yfinance_fetch(ticker, period="7d", interval="1d", retries=3, delay=1.0):
    for attempt in range(retries):
        try:
            df = ticker.history(period=period, interval=interval)
            if df is not None and not df.empty:
                return df
        except Exception:
            if attempt == retries - 1:
                raise
        time.sleep(delay)
    return None

def last_two_distinct_closes(df):
    """Return last two DISTINCT close values with their dates (most recent first)."""
    closes = df["Close"].dropna()
    if closes.shape[0] < 2:
        v = closes.iloc[-1] if not closes.empty else None
        return v, v
    last_val = closes.iloc[-1]
    prev_val = None
    # Walk back until a different value is found
    for v in reversed(closes.iloc[:-1].tolist()):
        if v != last_val:
            prev_val = v
            break
    if prev_val is None:
        # all same; fall back to previous row even if equal
        prev_val = closes.iloc[-2]
    return float(last_val), float(prev_val)

def closest_close_to_date(df, target_date):
    """Find close from the trading day nearest to target_date (date object)."""
    if df is None or df.empty:
        return None
    # df index is tz-aware DatetimeIndex; compare by .date()
    idx = min(df.index, key=lambda x: abs(x.date() - target_date))
    return float(df.loc[idx, "Close"])

def first_trading_close_on_or_after(df, start_date):
    """Get first available close on/after start_date (date object)."""
    if df is None or df.empty:
        return None
    for ts, row in df.iterrows():
        if ts.date() >= start_date:
            return float(row["Close"])
    # if nothing >= start_date, fall back to earliest
    return float(df["Close"].iloc[0])

# ---------- main ----------
def fetch_market_data() -> Optional[Dict[str, Any]]:
    try:
        now = datetime.now(SAST)
        one_day_ago = (now - timedelta(days=1)).strftime('%Y-%m-%d')
        one_month_ago = (now - timedelta(days=33)).strftime('%Y-%m-%d')  # 3-day buffer
        ytd_start = datetime(now.year, 1, 1).date()

        # Preserve your order: USDZAR before GOLD so conversion rate is available
        tickers = {
            "JSEALSHARE": "^J203.JO",
            "USDZAR": "USDZAR=X",
            "EURZAR": "EURZAR=X",
            "GBPZAR": "GBPZAR=X",
            "BRENT": "BZ=F",
            "GOLD": "GC=F",     # in USD/oz; we will convert to ZAR below
            "SP500": "^GSPC",
        }

        data: Dict[str, Any] = {}
        usdzar_today = None  # will be set when USDZAR is processed

        for label, symbol in tickers.items():
            try:
                t = yf.Ticker(symbol)

                # Daily window for 1D calculation
                daily = safe_yfinance_fetch(t, period="10d", interval="1d")
                if daily is None or daily.empty:
                    print(f"⚠️ No data for {label} ({symbol})")
                    continue

                # Last two DISTINCT closes (fixes +0.0% issue)
                today_val, day_ago_val = last_two_distinct_closes(daily)

                # Month window around target date (33-day span you already use)
                monthly = t.history(start=one_month_ago, end=one_day_ago)
                month_target = (now - timedelta(days=30)).date()
                month_ago_val = closest_close_to_date(monthly, month_target) if monthly is not None and not monthly.empty else None

                # YTD window (start from Jan 1, then first trading day on/after)
                ytd_hist = t.history(start=ytd_start.strftime('%Y-%m-%d'))
                ytd_val = first_trading_close_on_or_after(ytd_hist, ytd_start) if ytd_hist is not None and not ytd_hist.empty else None

                # Capture USDZAR for conversions
                if label == "USDZAR":
                    usdzar_today = today_val  # ZAR per USD

                # Convert GOLD (GC=F, USD/oz) to ZAR if USDZAR is known
                if label == "GOLD" and usdzar_today:
                    # Convert each base BEFORE computing % changes
                    today_val *= usdzar_today
                    if day_ago_val is not None:
                        day_ago_val *= usdzar_today
                    if month_ago_val is not None:
                        month_ago_val *= usdzar_today
                    if ytd_val is not None:
                        ytd_val *= usdzar_today

                # sanity guard for absurd YTDs (bad first tick)
                if ytd_val is not None and abs(calculate_percentage(ytd_val, today_val)) > 300:
                    ytd_val = None

                data[label] = {
                    "Today": float(today_val) if today_val is not None else 0.0,
                    "Change": calculate_percentage(day_ago_val, today_val),
                    "Monthly": calculate_percentage(month_ago_val, today_val),
                    "YTD": calculate_percentage(ytd_val, today_val) if ytd_val is not None else 0.0,
                }

            except Exception as e:
                print(f"⚠️ Error fetching {label}: {e}")
                continue

        # Timestamp/status
        data["timestamp"] = now.strftime("%d %b %Y, %H:%M")
        data["data_status"] = "complete" if all(k in data for k in tickers) else "partial"
        return data

    except Exception as e:
        print(f"❌ Critical error in fetch_market_data: {e}")
        return None
