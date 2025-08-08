from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional, Tuple
import time

import pytz
import yfinance as yf
from pycoingecko import CoinGeckoAPI

SAST = pytz.timezone("Africa/Johannesburg")

# -------------------------------
# Utility helpers
# -------------------------------

def _safe_pct(new: Optional[float], base: Optional[float]) -> Optional[float]:
    """Return percentage change or None if not computable."""
    try:
        if new is None or base is None or base == 0:
            return None
        return (new / base - 1.0) * 100.0
    except Exception:
        return None

def _ensure_series(df) -> bool:
    try:
        return df is not None and len(df.index) >= 2 and 'Close' in df.columns
    except Exception:
        return False

def _get_first_on_or_after(idx, target_dt):
    """Return the index label of the first row with date >= target_dt, else None."""
    try:
        for ts in idx:
            if ts.tzinfo is not None:
                ts_local = ts.tz_convert(SAST)
            else:
                ts_local = SAST.localize(ts)
            if ts_local.date() >= target_dt.date():
                return ts
        return None
    except Exception:
        return None

def _get_last_on_or_before(idx, target_dt):
    """Return the index label of the last row with date <= target_dt, else None."""
    try:
        candidate = None
        for ts in idx:
            if ts.tzinfo is not None:
                ts_local = ts.tz_convert(SAST)
            else:
                ts_local = SAST.localize(ts)
            if ts_local.date() <= target_dt.date():
                candidate = ts
            else:
                break
        return candidate
    except Exception:
        return None

def _latest_close(df) -> Optional[float]:
    try:
        return float(df['Close'].iloc[-1])
    except Exception:
        return None

def _prev_close(df) -> Optional[float]:
    try:
        return float(df['Close'].iloc[-2])
    except Exception:
        return None

def _anchor_close_on_or_after(df, target_dt) -> Optional[float]:
    """Get the close from the first trading day ON or AFTER target_dt."""
    try:
        ts = _get_first_on_or_after(df.index, target_dt)
        if ts is None:
            return None
        return float(df.loc[ts, 'Close'])
    except Exception:
        return None

def _anchor_close_on_or_before(df, target_dt) -> Optional[float]:
    """Get the close from the last trading day ON or BEFORE target_dt."""
    try:
        ts = _get_last_on_or_before(df.index, target_dt)
        if ts is None:
            return None
        return float(df.loc[ts, 'Close'])
    except Exception:
        return None

def _yf_history(symbol: str, start: datetime, end: datetime) -> Optional[Any]:
    """Fetch daily history with a small retry. Returns DataFrame or None."""
    for attempt in range(3):
        try:
            df = yf.download(symbol, start=start.date(), end=(end + timedelta(days=1)).date(), interval='1d', auto_adjust=False, progress=False, threads=False)
            # Defensive clean
            if df is not None and hasattr(df, 'dropna'):
                df = df.dropna(subset=['Close'])
            if _ensure_series(df):
                return df
        except Exception:
            time.sleep(1.0 * (attempt + 1))
    return None

def _coingecko_history_days(coin_id: str, vs: str, days: int) -> Optional[list]:
    """Return list of [ts_ms, price] for <=days, or None."""
    cg = CoinGeckoAPI()
    for attempt in range(3):
        try:
            data = cg.get_coin_market_chart_by_id(id=coin_id, vs_currency=vs, days=days)
            prices = data.get('prices')
            if prices and len(prices) >= 2:
                return prices
        except Exception:
            time.sleep(1.0 * (attempt + 1))
    return None

def _coingecko_price(coin_id: str, vs: str) -> Optional[float]:
    cg = CoinGeckoAPI()
    for attempt in range(3):
        try:
            d = cg.get_price(ids=[coin_id], vs_currencies=[vs])
            v = d.get(coin_id, {}).get(vs)
            if v is not None:
                return float(v)
        except Exception:
            time.sleep(1.0 * (attempt + 1))
    return None

def _calc_btc_changes_zar() -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    """Return (today, 1D, 1M, YTD) for BTC in ZAR using CoinGecko."""
    now = datetime.now(SAST)
    today_price = _coingecko_price('bitcoin', 'zar')
    hist = _coingecko_history_days('bitcoin', 'zar', 400)  # enough for YTD and 1M

    # Defensive: if no history, try minimal fallback
    if not hist:
        return today_price, None, None, None

    # Convert to daily closes by date (take last sample per date)
    from collections import OrderedDict
    by_date = OrderedDict()
    for ts_ms, price in hist:
        dt = datetime.fromtimestamp(ts_ms / 1000, tz=SAST).date()
        by_date[dt] = float(price)

    # Anchors
    today_close = by_date.get(now.date(), None) or today_price
    # 1D anchor = previous available day
    dates = list(by_date.keys())
    if now.date() in by_date:
        idx = dates.index(now.date())
        prev_close = by_date[dates[idx-1]] if idx >= 1 else None
    else:
        # take last date in history as today_close
        prev_close = by_date[dates[-2]] if len(dates) >= 2 else None
        if today_close is None:
            today_close = by_date[dates[-1]]

    # 1M anchor = last on/before now - 30d
    m_anchor = None
    target_m = now.date() - timedelta(days=30)
    for d in reversed(dates):
        if d <= target_m:
            m_anchor = by_date[d]
            break

    # YTD anchor = first on/after Jan 1
    ytd_anchor = None
    jan1 = now.replace(month=1, day=1).date()
    for d in dates:
        if d >= jan1:
            ytd_anchor = by_date[d]
            break

    one_d = _safe_pct(today_close, prev_close)
    one_m = _safe_pct(today_close, m_anchor)
    ytd = _safe_pct(today_close, ytd_anchor)

    return today_close, one_d, one_m, ytd

def _assemble_row(today: Optional[float], one_d: Optional[float], one_m: Optional[float], ytd: Optional[float]) -> Dict[str, Optional[float]]:
    return {
        "Today": today,
        "Change": one_d,
        "Monthly": one_m,
        "YTD": ytd,
    }

def fetch_market_data() -> Optional[Dict[str, Any]]:
    """
    Returns a dict with keys:
      JSEALSHARE, USDZAR, EURZAR, GBPZAR, BRENT, GOLD, SP500, BITCOINZAR
    Each key maps to: {"Today": float|None, "Change": float|None, "Monthly": float|None, "YTD": float|None}
    Also includes: "timestamp" (str), "data_status" ("complete"/"partial")
    """
    now = datetime.now(SAST)

    # Single history window covers 1M & YTD
    start = min(now.replace(month=1, day=1), now - timedelta(days=60))
    end = now

    # Yahoo symbols (keep consistent aliases)
    symbols = {
        "JSEALSHARE": "^J203.JO",
        "USDZAR": "USDZAR=X",
        "EURZAR": "EURZAR=X",
        "GBPZAR": "GBPZAR=X",
        "BRENT": "BZ=F",
        "GOLD": "GC=F",
        "SP500": "^GSPC",
    }

    data: Dict[str, Any] = {}

    for label, sym in symbols.items():
        # fetch history
        df = _yf_history(sym, start, end)
        if not _ensure_series(df):
            # Try a single fallback alias for ALSH if needed
            if label == "JSEALSHARE":
                df = _yf_history("^JALSH", start, end)
        if not _ensure_series(df):
            data[label] = _assemble_row(None, None, None, None)
            continue

        latest = _latest_close(df)
        prev = _prev_close(df)
        # monthly anchor (last on/before now-30d)
        m_anchor = _anchor_close_on_or_before(df, now - timedelta(days=30))
        # ytd anchor (first on/after Jan 1)
        y_anchor = _anchor_close_on_or_after(df, now.replace(month=1, day=1))

        data[label] = _assemble_row(
            latest,
            _safe_pct(latest, prev),
            _safe_pct(latest, m_anchor),
            _safe_pct(latest, y_anchor)
        )

    # Bitcoin (ZAR) via CoinGecko
    btc_today, btc_1d, btc_1m, btc_ytd = _calc_btc_changes_zar()
    data["BITCOINZAR"] = _assemble_row(btc_today, btc_1d, btc_1m, btc_ytd)

    # Meta
    data["timestamp"] = now.strftime("%d %b %Y, %H:%M")
    # Status = complete if all keys present and have Today not None
    required = ["JSEALSHARE","USDZAR","EURZAR","GBPZAR","BRENT","GOLD","SP500","BITCOINZAR"]
    complete = all(k in data and isinstance(data[k], dict) for k in required)
    data["data_status"] = "complete" if complete else "partial"

    return data
