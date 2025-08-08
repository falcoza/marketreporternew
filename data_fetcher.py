from __future__ import annotations

from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Tuple, List
import time

import pytz
import yfinance as yf
from pycoingecko import CoinGeckoAPI
import requests

SAST = pytz.timezone("Africa/Johannesburg")


# -------------------------------
# Small logging helper
# -------------------------------

def _log(msg: str) -> None:
    print(f"[data_fetcher] {msg}")


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
        return (
            df is not None
            and len(df.index) >= 2
            and 'Close' in df.columns
            and df['Close'].notna().sum() >= 2
        )
    except Exception:
        return False


def _latest_close(df) -> Optional[float]:
    try:
        return float(df['Close'].dropna().iloc[-1])
    except Exception:
        return None


def _prev_close(df) -> Optional[float]:
    try:
        return float(df['Close'].dropna().iloc[-2])
    except Exception:
        return None


def _get_first_on_or_after(idx, target_dt):
    """Return the index label of the first row with date >= target_dt, else None."""
    try:
        for ts in idx:
            ts_local = ts.tz_convert(SAST) if ts.tzinfo is not None else SAST.localize(ts)
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
            ts_local = ts.tz_convert(SAST) if ts.tzinfo is not None else SAST.localize(ts)
            if ts_local.date() <= target_dt.date():
                candidate = ts
            else:
                break
        return candidate
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


# -------------------------------
# Yahoo Finance helpers (multi-strategy)
# -------------------------------

def _yf_download(symbol: str, start: datetime, end: datetime):
    """Try multiple download strategies for Yahoo."""
    # Strategy 1: start/end window
    for attempt in range(2):
        try:
            df = yf.download(
                symbol,
                start=start.date(),
                end=(end + timedelta(days=1)).date(),
                interval='1d',
                auto_adjust=False,
                progress=False,
                threads=False
            )
            if _ensure_series(df):
                _log(f"Yahoo OK ({symbol}) via start/end window")
                return df
            else:
                _log(f"Yahoo empty ({symbol}) via start/end window [attempt {attempt+1}]")
        except Exception as e:
            _log(f"Yahoo error ({symbol}) window [attempt {attempt+1}]: {e}")
            time.sleep(0.8 * (attempt + 1))

    # Strategy 2: period=ytd
    try:
        df = yf.download(symbol, period="ytd", interval="1d", auto_adjust=False, progress=False, threads=False)
        if _ensure_series(df):
            _log(f"Yahoo OK ({symbol}) via period=ytd")
            return df
        else:
            _log(f"Yahoo empty ({symbol}) via period=ytd")
    except Exception as e:
        _log(f"Yahoo error ({symbol}) period=ytd: {e}")

    # Strategy 3: period=1y
    try:
        df = yf.download(symbol, period="1y", interval="1d", auto_adjust=False, progress=False, threads=False)
        if _ensure_series(df):
            _log(f"Yahoo OK ({symbol}) via period=1y")
            return df
        else:
            _log(f"Yahoo empty ({symbol}) via period=1y")
    except Exception as e:
        _log(f"Yahoo error ({symbol}) period=1y: {e}")

    return None


def _yf_history_multi(candidates: List[str], start: datetime, end: datetime):
    """Try multiple symbols and strategies; return (df, symbol_used)."""
    for sym in candidates:
        df = _yf_download(sym, start, end)
        if _ensure_series(df):
            return df, sym
    return None, None


# -------------------------------
# FX fallback via exchangerate.host
# -------------------------------

def _fx_timeseries(base: str, quote: str, start_date: datetime, end_date: datetime) -> Optional[Dict[datetime, float]]:
    """Fetch daily FX rates base/quote; returns dict of date->rate. Uses exchangerate.host as fallback."""
    try:
        url = "https://api.exchangerate.host/timeseries"
        params = {
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "base": base,
            "symbols": quote,
        }
        r = requests.get(url, params=params, timeout=10)
        if r.status_code != 200:
            _log(f"FX fallback HTTP {r.status_code} for {base}{quote}")
            return None
        data = r.json()
        if not data.get("success"):
            _log(f"FX fallback returned success=False for {base}{quote}")
            return None
        rates = data.get("rates", {})
        out = {}
        for dstr, vec in sorted(rates.items()):
            val = vec.get(quote)
            if val is not None:
                dt = datetime.strptime(dstr, "%Y-%m-%d").date()
                out[dt] = float(val)
        if len(out) < 2:
            _log(f"FX fallback had insufficient data for {base}{quote}")
            return None
        _log(f"FX fallback OK via exchangerate.host for {base}{quote}")
        return out
    except Exception as e:
        _log(f"FX fallback error for {base}{quote}: {e}")
        return None


# -------------------------------
# BTC (ZAR) via CoinGecko
# -------------------------------

def _coingecko_history_days(coin_id: str, vs: str, days: int) -> Optional[list]:
    cg = CoinGeckoAPI()
    for attempt in range(3):
        try:
            data = cg.get_coin_market_chart_by_id(id=coin_id, vs_currency=vs, days=days)
            prices = data.get('prices')
            if prices and len(prices) >= 2:
                if attempt > 0:
                    _log(f"CoinGecko recovered for {coin_id}-{vs} on attempt {attempt+1}")
                return prices
            _log(f"CoinGecko empty history for {coin_id}-{vs} [attempt {attempt+1}]")
        except Exception as e:
            _log(f"CoinGecko error {coin_id}-{vs} [attempt {attempt+1}]: {e}")
            time.sleep(1.0 * (attempt + 1))
    return None


def _coingecko_price(coin_id: str, vs: str) -> Optional[float]:
    cg = CoinGeckoAPI()
    for attempt in range(3):
        try:
            d = cg.get_price(ids=[coin_id], vs_currencies=[vs])
            v = d.get(coin_id, {}).get(vs)
            if v is not None:
                if attempt > 0:
                    _log(f"CoinGecko recovered spot for {coin_id}-{vs} on attempt {attempt+1}")
                return float(v)
            _log(f"CoinGecko empty spot for {coin_id}-{vs} [attempt {attempt+1}]")
        except Exception as e:
            _log(f"CoinGecko spot error {coin_id}-{vs} [attempt {attempt+1}]: {e}")
            time.sleep(1.0 * (attempt + 1))
    return None


def _calc_btc_changes_zar() -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    now = datetime.now(SAST)
    today_price = _coingecko_price('bitcoin', 'zar')
    hist = _coingecko_history_days('bitcoin', 'zar', 400)
    if not hist:
        _log("BTC history unavailable from CoinGecko")
        return today_price, None, None, None

    from collections import OrderedDict
    by_date = OrderedDict()
    for ts_ms, price in hist:
        dt = datetime.fromtimestamp(ts_ms / 1000, tz=SAST).date()
        by_date[dt] = float(price)

    today_close = by_date.get(now.date(), None) or today_price
    dates = list(by_date.keys())
    if now.date() in by_date:
        idx = dates.index(now.date())
        prev_close = by_date[dates[idx-1]] if idx >= 1 else None
    else:
        prev_close = by_date[dates[-2]] if len(dates) >= 2 else None
        if today_close is None:
            today_close = by_date[dates[-1]]

    target_m = now.date() - timedelta(days=30)
    m_anchor = None
    for d in reversed(dates):
        if d <= target_m:
            m_anchor = by_date[d]
            break

    jan1 = now.replace(month=1, day=1).date()
    ytd_anchor = None
    for d in dates:
        if d >= jan1:
            ytd_anchor = by_date[d]
            break

    one_d = _safe_pct(today_close, prev_close)
    one_m = _safe_pct(today_close, m_anchor)
    ytd = _safe_pct(today_close, ytd_anchor)

    return today_close, one_d, one_m, ytd


# -------------------------------
# Assemble rows
# -------------------------------

def _assemble_row(today: Optional[float], one_d: Optional[float], one_m: Optional[float], ytd: Optional[float]) -> Dict[str, Optional[float]]:
    return {"Today": today, "Change": one_d, "Monthly": one_m, "YTD": ytd}


# -------------------------------
# Public fetcher
# -------------------------------

def fetch_market_data() -> Optional[Dict[str, Any]]:
    now = datetime.now(SAST)
    # Single window to cover 1M and YTD anchors
    start = min(now.replace(month=1, day=1), now - timedelta(days=60))
    end = now

    # Candidates per instrument (left to right = preference order)
    candidates = {
        "JSEALSHARE": ["^J203.JO", "^JALSH", "^J200.JO"],  # last resort proxy: Top40
        "USDZAR": ["USDZAR=X"],
        "EURZAR": ["EURZAR=X"],
        "GBPZAR": ["GBPZAR=X"],
        "BRENT": ["BZ=F", "BRN=F", "CO=F"],
        "GOLD": ["GC=F", "XAUUSD=X"],
        "SP500": ["^GSPC", "^SPX", "SPY"],
    }

    data: Dict[str, Any] = {}

    # Yahoo path for each instrument
    for label, syms in candidates.items():
        df, used = _yf_history_multi(syms, start, end)
        if _ensure_series(df):
            latest = _latest_close(df)
            prev = _prev_close(df)
            m_anchor = _anchor_close_on_or_before(df, now - timedelta(days=30))
            y_anchor = _anchor_close_on_or_after(df, now.replace(month=1, day=1))

            row = _assemble_row(
                latest,
                _safe_pct(latest, prev),
                _safe_pct(latest, m_anchor),
                _safe_pct(latest, y_anchor)
            )
            row["_source"] = f"Yahoo:{used}"
            data[label] = row
            continue

        # FX explicit fallback via exchangerate.host
        if label in ("USDZAR", "EURZAR", "GBPZAR"):
            base = label[:3]  # USD/EUR/GBP
            quote = "ZAR"
            series = _fx_timeseries(base, quote, start, end)
            if series and len(series) >= 2:
                dates = sorted(series.keys())
                today_rate = series.get(now.date()) or series.get(dates[-1])
                prev_rate = series.get(dates[-2]) if len(dates) >= 2 else None

                # 1M anchor (last on/before now-30d)
                target_m = now.date() - timedelta(days=30)
                m_anchor = None
                for d in reversed(dates):
                    if d <= target_m:
                        m_anchor = series[d]
                        break

                # YTD anchor (first on/after Jan 1)
                ytd_anchor = None
                jan1 = now.replace(month=1, day=1).date()
                for d in dates:
                    if d >= jan1:
                        ytd_anchor = series[d]
                        break

                row = _assemble_row(
                    today_rate, _safe_pct(today_rate, prev_rate), _safe_pct(today_rate, m_anchor), _safe_pct(today_rate, ytd_anchor)
                )
                row["_source"] = "exchangerate.host"
                data[label] = row
                continue

        # If still nothing, set Nones
        _log(f"No data for {label} after all attempts; setting N/A")
        data[label] = _assemble_row(None, None, None, None)
        data[label]["_source"] = "unavailable"

    # Bitcoin (ZAR)
    btc_today, btc_1d, btc_1m, btc_ytd = _calc_btc_changes_zar()
    btc_row = _assemble_row(btc_today, btc_1d, btc_1m, btc_ytd)
    btc_row["_source"] = "CoinGecko"
    data["BITCOINZAR"] = btc_row

    # Meta
    data["timestamp"] = now.strftime("%d %b %Y, %H:%M")

    # Status = complete if all keys present (values may still be None)
    required = ["JSEALSHARE","USDZAR","EURZAR","GBPZAR","BRENT","GOLD","SP500","BITCOINZAR"]
    complete = all(k in data and isinstance(data[k], dict) for k in required)
    data["data_status"] = "complete" if complete else "partial"

    # Optional: warn on anchors that were None (helps trace odd %s)
    for k in required:
        row = data.get(k, {})
        for anchor_name in ("Change", "Monthly", "YTD"):
            if row.get(anchor_name) is None:
                _log(f"Anchor '{anchor_name}' missing for {k} (source={row.get('_source')})")

    return data
