from datetime import datetime, timedelta
from typing import Optional, Dict, Any
import time
import math
import pytz
import yfinance as yf
# from pycoingecko import CoinGeckoAPI  # kept for BTC if you use it

SAST = pytz.timezone("Africa/Johannesburg")

# ---------- helpers ----------
def _is_num(x) -> bool:
    return x is not None and not (isinstance(x, float) and math.isnan(x))

def calculate_percentage(old: Optional[float], new: Optional[float]) -> float:
    if not _is_num(old) or not _is_num(new) or old == 0:
        return 0.0
    try:
        return ((new - old) / old) * 100.0
    except Exception:
        return 0.0

def safe_yfinance_fetch(ticker, period="10d", interval="1d", retries=3, delay=1.0):
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

def _dates_sast(index):
    """Return list of date() in SAST from a DatetimeIndex (tz-aware or naive)."""
    try:
        if index.tz is not None:
            return [ts.tz_convert(SAST).date() for ts in index]
    except Exception:
        pass
    # naive index or conversion failed: assume UTC ~ fine for day bars
    return [ts.date() for ts in index]

def last_two_distinct_completed_closes(daily_df, now_sast: datetime):
    """
    Use the last TWO completed trading-day closes.
    If Yahoo has inserted a provisional 'today' bar before ~17:10 SAST, drop it.
    Then walk back to find two distinct closes.
    """
    closes = daily_df["Close"].dropna()
    if closes.empty:
        return None, None

    dates = _dates_sast(closes.index)
    # If last row is today and session likely not closed yet, drop it
    last_is_today = dates[-1] == now_sast.date()
    # conservative close cut-off ~17:10 SAST
    session_complete = (now_sast.hour, now_sast.minute) >= (17, 10)
    if last_is_today and not session_complete and len(closes) > 1:
        closes = closes.iloc[:-1]
        dates = dates[:-1]

    # Need at least two rows after possible drop
    if len(closes) == 1:
        v = float(closes.iloc[0])
        return v, v
    last_val = float(closes.iloc[-1])
    prev_val = None
    for v in reversed(closes.iloc[:-1].tolist()):
        if v != last_val:
            prev_val = float(v)
            break
    if prev_val is None:
        prev_val = float(closes.iloc[-2])
    return last_val, prev_val

def closest_close_to_date(df, target_date):
    """Find close from the trading day nearest to target_date (date object)."""
    if df is None or df.empty:
        return None
    idx = min(df.index, key=lambda x: abs(x.date() - target_date))
    v = df.loc[idx, "Close"]
    return float(v) if _is_num(v) else None

def first_trading_close_on_or_after(df, start_date):
    """Get first available close on/after start_date (date object)."""
    if df is None or df.empty:
        return None
    for ts, row in df.iterrows():
        if ts.date() >= start_date and _is_num(row.get("Close")):
            return float(row["Close"])
    # fallback to earliest valid
    v = df["Close"].dropna()
    return float(v.iloc[0]) if not v.empty else None

# ---------- main ----------
def fetch_market_data() -> Optional[Dict[str, Any]]:
    try:
        now = datetime.now(SAST)
        # wider month window avoids edge clipping; end left open
        month_window_start = (now - timedelta(days=60)).strftime('%Y-%m-%d')
        ytd_start = datetime(now.year, 1, 1).date()

        tickers = {
            "JSEALSHARE": "^J203.JO",
            "USDZAR": "USDZAR=X",
            "EURZAR": "EURZAR=X",
            "GBPZAR": "GBPZAR=X",
            "BRENT": "BZ=F",
            "GOLD": "GC=F",     # USD/oz
            "SP500": "^GSPC",
        }

        data: Dict[str, Any] = {}
        usdzar_today = None  # captured to convert GOLD

        for label, symbol in tickers.items():
            try:
                t = yf.Ticker(symbol)

                # Daily series for 1D change with completion guard
                daily = safe_yfinance_fetch(t, period="15d", interval="1d")
                if daily is None or daily.empty:
                    print(f"⚠️ No data for {label} ({symbol})")
                    continue

                today_val, day_ago_val = last_two_distinct_completed_closes(daily, now)

                # Month window; pick closest to target 30D ago
                monthly = t.history(start=month_window_start)  # leave end open
                month_target = (now - timedelta(days=30)).date()
                month_ago_val = closest_close_to_date(monthly, month_target) if monthly is not None and not monthly.empty else None

                # YTD: first trading close on/after Jan 1
                ytd_hist = t.history(start=ytd_start.strftime('%Y-%m-%d'))
                ytd_val = first_trading_close_on_or_after(ytd_hist, ytd_start) if ytd_hist is not None and not ytd_hist.empty else None

                # Capture USDZAR for conversions
                if label == "USDZAR" and _is_num(today_val):
                    usdzar_today = today_val  # ZAR per USD

                # GOLD conversion to ZAR (only if we have a valid USDZAR)
                if label == "GOLD" and _is_num(usdzar_today):
                    conv = usdzar_today
                    today_val     = float(today_val) * conv if _is_num(today_val) else None
                    day_ago_val   = float(day_ago_val) * conv if _is_num(day_ago_val) else None
                    month_ago_val = float(month_ago_val) * conv if _is_num(month_ago_val) else None
                    ytd_val       = float(ytd_val) * conv if _is_num(ytd_val) else None

                # sanity guard for absurd YTDs (bad first tick)
                if _is_num(ytd_val) and _is_num(today_val):
                    if abs(calculate_percentage(ytd_val, today_val)) > 300:
                        ytd_val = None

                data[label] = {
                    "Today": float(today_val) if _is_num(today_val) else 0.0,
                    "Change": calculate_percentage(day_ago_val, today_val),
                    "Monthly": calculate_percentage(month_ago_val, today_val),
                    "YTD": calculate_percentage(ytd_val, today_val) if _is_num(ytd_val) else 0.0,
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
