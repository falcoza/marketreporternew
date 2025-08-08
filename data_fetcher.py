from __future__ import annotations

from datetime import datetime, timedelta, date
from typing import Dict, Any, Optional, Tuple, List
import io
import random
import time

import pandas as pd
import pytz
import requests
import yfinance as yf
from pycoingecko import CoinGeckoAPI

SAST = pytz.timezone("Africa/Johannesburg")

# =========================
# Logging
# =========================
def _log(msg: str) -> None:
    print(f"[data_fetcher] {msg}")

# =========================
# Scalar & DF helpers
# =========================
def _as_float(x):
    """Convert scalar-like or 1-element Series/Index to float safely."""
    import numpy as np
    if isinstance(x, (pd.Series, pd.Index)):
        if len(x) == 0:
            return None
        x = x.iloc[0]
    if hasattr(x, "item"):  # numpy scalar -> Python scalar
        try:
            return x.item()
        except Exception:
            pass
    try:
        return float(x)
    except Exception:
        return None

def _ensure_df(df: Optional[pd.DataFrame]) -> bool:
    try:
        return (
            isinstance(df, pd.DataFrame)
            and "Close" in df.columns
            and df["Close"].dropna().shape[0] >= 2
        )
    except Exception:
        return False

def _dedup(df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    if df is None or not isinstance(df, pd.DataFrame):
        return df
    try:
        df = df[~df.index.duplicated(keep="last")]
    except Exception:
        pass
    return df

def _latest_close(df: pd.DataFrame) -> Optional[float]:
    try:
        val = df["Close"].dropna().iloc[-1]
        return _as_float(val)
    except Exception:
        return None

def _prev_close(df: pd.DataFrame) -> Optional[float]:
    try:
        val = df["Close"].dropna().iloc[-2]
        return _as_float(val)
    except Exception:
        return None

def _first_on_or_after(df: pd.DataFrame, target_dt: datetime) -> Optional[float]:
    """Close from first trading day ON/AFTER target."""
    try:
        for ts in df.index:
            tloc = ts.tz_convert(SAST) if ts.tzinfo is not None else SAST.localize(ts)
            if tloc.date() >= target_dt.date():
                return _as_float(df.loc[ts, "Close"])
        return None
    except Exception:
        return None

def _last_on_or_before(df: pd.DataFrame, target_dt: datetime) -> Optional[float]:
    """Close from last trading day ON/BEFORE target."""
    try:
        out = None
        for ts in df.index:
            tloc = ts.tz_convert(SAST) if ts.tzinfo is not None else SAST.localize(ts)
            if tloc.date() <= target_dt.date():
                out = _as_float(df.loc[ts, "Close"])
            else:
                break
        return out
    except Exception:
        return None

def _safe_pct(new: Optional[float], base: Optional[float]) -> Optional[float]:
    try:
        if new is None or base is None or base == 0:
            return None
        return (new / base - 1.0) * 100.0
    except Exception:
        return None

def _assemble_row(today: Optional[float], one_d: Optional[float],
                  one_m: Optional[float], ytd: Optional[float]) -> Dict[str, Optional[float]]:
    return {"Today": today, "Change": one_d, "Monthly": one_m, "YTD": ytd}

# =========================
# Yahoo (CSV first, then yfinance)
# =========================
_UAS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Mozilla/5.0 (X11; Linux x86_64)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
]

def _yahoo_csv(symbol: str, start: datetime, end: datetime, retries: int = 2) -> Optional[pd.DataFrame]:
    """Hit Yahoo's CSV endpoint; often more stable than yfinance cookie flow."""
    for attempt in range(retries):
        try:
            p1 = int(start.timestamp())
            # Yahoo usually excludes the end day unless you add +1 buffer
            p2 = int((end + timedelta(days=1)).timestamp())
            url = (
                f"https://query1.finance.yahoo.com/v7/finance/download/{symbol}"
                f"?period1={p1}&period2={p2}&interval=1d&events=history&includeAdjustedClose=true"
            )
            headers = {
                "User-Agent": random.choice(_UAS),
                "Accept": "text/csv,application/json;q=0.9,*/*;q=0.8",
            }
            r = requests.get(url, headers=headers, timeout=15)
            if r.status_code != 200 or "Date,Open,High,Low,Close" not in r.text:
                _log(f"Yahoo CSV miss ({symbol}) http={r.status_code}")
                continue
            df = pd.read_csv(io.StringIO(r.text))
            if "Date" not in df.columns or "Close" not in df.columns:
                _log(f"Yahoo CSV malformed ({symbol})")
                continue
            df["Date"] = pd.to_datetime(df["Date"], utc=True)
            df = df.set_index("Date").sort_index()
            df = _dedup(df)
            if _ensure_df(df):
                _log(f"Yahoo CSV OK ({symbol})")
                return df
            _log(f"Yahoo CSV empty ({symbol}) after parse")
        except Exception as e:
            _log(f"Yahoo CSV error ({symbol}) attempt {attempt+1}: {e}")
        time.sleep(0.5 + attempt * 0.5)
    return None

def _yahoo_yf(symbol: str, start: datetime, end: datetime) -> Optional[pd.DataFrame]:
    """yfinance fallback with multiple strategies; bail fast on known internal errors."""
    # Strategy 1: window
    try:
        df = yf.download(
            symbol,
            start=start.date(),
            end=(end + timedelta(days=1)).date(),
            interval="1d",
            auto_adjust=False,
            progress=False,
            threads=False,
        )
        df = _dedup(df)
        if _ensure_df(df):
            _log(f"yfinance OK ({symbol}) window")
            return df
        _log(f"yfinance empty ({symbol}) window")
    except Exception as e:
        _log(f"yfinance error ({symbol}) window: {e}")

    # Strategy 2: period=ytd
    try:
        df = yf.download(symbol, period="ytd", interval="1d",
                         auto_adjust=False, progress=False, threads=False)
        df = _dedup(df)
        if _ensure_df(df):
            _log(f"yfinance OK ({symbol}) period=ytd")
            return df
        _log(f"yfinance empty ({symbol}) period=ytd")
    except Exception as e:
        _log(f"yfinance error ({symbol}) period=ytd: {e}")

    # Strategy 3: period=1y
    try:
        df = yf.download(symbol, period="1y", interval="1d",
                         auto_adjust=False, progress=False, threads=False)
        df = _dedup(df)
        if _ensure_df(df):
            _log(f"yfinance OK ({symbol}) period=1y")
            return df
        _log(f"yfinance empty ({symbol}) period=1y")
    except Exception as e:
        _log(f"yfinance error ({symbol}) period=1y: {e}")

    return None

def _get_from_yahoo(symbol: str, start: datetime, end: datetime) -> Optional[pd.DataFrame]:
    """Preferred: CSV → yfinance."""
    df = _yahoo_csv(symbol, start, end)
    if _ensure_df(df):
        return df
    return _yahoo_yf(symbol, start, end)

# =========================
# Stooq & Frankfurter backups (quiet, only if Yahoo fails)
# =========================
def _stooq_csv(symbol: str) -> Optional[pd.DataFrame]:
    try:
        url = f"https://stooq.com/q/d/l/?s={symbol}&i=d"
        r = requests.get(url, timeout=15)
        if r.status_code != 200 or "Date,Open,High,Low,Close,Volume" not in r.text:
            _log(f"Stooq CSV miss ({symbol}) http={r.status_code}")
            return None
        df = pd.read_csv(io.StringIO(r.text))
        if "Date" not in df.columns or "Close" not in df.columns:
            _log(f"Stooq CSV malformed ({symbol})")
            return None
        df["Date"] = pd.to_datetime(df["Date"], utc=True)
        df = df.set_index("Date").sort_index()
        df = _dedup(df)
        return df if _ensure_df(df) else None
    except Exception as e:
        _log(f"Stooq CSV error ({symbol}): {e}")
        return None

def _frankfurter_series(base: str, quote: str, start_d: date, end_d: date) -> Optional[pd.DataFrame]:
    try:
        url = f"https://api.frankfurter.dev/v1/{start_d.isoformat()}..{end_d.isoformat()}?base={base}&symbols={quote}"
        r = requests.get(url, timeout=15)
        if r.status_code != 200:
            _log(f"Frankfurter http={r.status_code} {base}{quote}")
            return None
        payload = r.json()
        rates = payload.get("rates", {})
        if not rates:
            _log(f"Frankfurter empty {base}{quote}")
            return None
        rows = []
        for dstr, m in rates.items():
            if quote in m:
                rows.append((pd.to_datetime(dstr, utc=True), float(m[quote])))
        if len(rows) < 2:
            return None
        df = pd.DataFrame(rows, columns=["Date", "Close"]).set_index("Date").sort_index()
        df = _dedup(df)
        return df if _ensure_df(df) else None
    except Exception as e:
        _log(f"Frankfurter error {base}{quote}: {e}")
        return None

# =========================
# Bitcoin via CoinGecko (≤ 365 days)
# =========================
def _btc_zar_ytd() -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    cg = CoinGeckoAPI()
    now = datetime.now(SAST)
    jan1 = date(now.year, 1, 1)
    days = min(365, (now.date() - jan1).days + 2)

    # spot
    spot = None
    for attempt in range(3):
        try:
            spot_d = cg.get_price(ids=["bitcoin"], vs_currencies=["zar"])
            spot = float(spot_d["bitcoin"]["zar"])
            break
        except Exception as e:
            _log(f"CoinGecko spot error attempt {attempt+1}: {e}")
            time.sleep(1 + attempt)

    # history
    prices = None
    for attempt in range(3):
        try:
            hist = cg.get_coin_market_chart_by_id(id="bitcoin", vs_currency="zar", days=days)
            prices = hist.get("prices") if hist else None
            if prices:
                break
            _log(f"CoinGecko history empty attempt {attempt+1}")
        except Exception as e:
            _log(f"CoinGecko history error attempt {attempt+1}: {e}")
            time.sleep(1 + attempt)

    if not prices:
        _log("BTC history unavailable from CoinGecko")
        return spot, None, None, None

    # one close per day (last sample per calendar date)
    by_date: Dict[date, float] = {}
    for ts_ms, px in prices:
        d = datetime.fromtimestamp(ts_ms / 1000, tz=SAST).date()
        by_date[d] = float(px)
    dates = sorted(by_date.keys())
    today_close = by_date.get(now.date(), by_date.get(dates[-1], spot))
    prev_close = by_date.get(dates[-2]) if len(dates) >= 2 else None

    target_m = now.date() - timedelta(days=30)
    m_anchor = None
    for d in reversed(dates):
        if d <= target_m:
            m_anchor = by_date[d]
            break

    ytd_anchor = None
    for d in dates:
        if d >= jan1:
            ytd_anchor = by_date[d]
            break

    return (
        today_close,
        _safe_pct(today_close, prev_close),
        _safe_pct(today_close, m_anchor),
        _safe_pct(today_close, ytd_anchor),
    )

# =========================
# Public: fetch all instruments
# =========================
def fetch_market_data() -> Optional[Dict[str, Any]]:
    now = datetime.now(SAST)
    # Single window to cover 1M and YTD anchors
    start = min(now.replace(month=1, day=1), now - timedelta(days=60))
    end = now

    # Verified Yahoo symbols
    YAHOO = {
        "JSEALSHARE": "^J203.JO",     # FTSE/JSE All Share Index
        "USDZAR": "USDZAR=X",
        "EURZAR": "EURZAR=X",
        "GBPZAR": "GBPZAR=X",
        "BRENT": "BZ=F",              # Brent futures continuous
        "GOLD": "GC=F",               # Gold futures continuous
        "SP500": "^GSPC",             # S&P 500 Index
    }

    # Backup symbols on Stooq (CSV) if Yahoo fails completely
    STOOQ = {
        "BRENT": "cb.f",
        "GOLD": "gc.f",
        "SP500": "^spx",
    }

    data: Dict[str, Any] = {}

    # ---- JSE ALSH (Yahoo only – there isn't a good free backup) ----
    sym = YAHOO["JSEALSHARE"]
    df = _get_from_yahoo(sym, start, end)
    if _ensure_df(df):
        latest = _latest_close(df)
        prev = _prev_close(df)
        m_anchor = _last_on_or_before(df, now - timedelta(days=30))
        y_anchor = _first_on_or_after(df, now.replace(month=1, day=1))
        data["JSEALSHARE"] = _assemble_row(latest, _safe_pct(latest, prev),
                                            _safe_pct(latest, m_anchor), _safe_pct(latest, y_anchor))
        data["JSEALSHARE"]["_source"] = f"Yahoo:{sym}"
    else:
        _log("No data for JSEALSHARE after Yahoo CSV+yfinance")
        data["JSEALSHARE"] = _assemble_row(None, None, None, None)
        data["JSEALSHARE"]["_source"] = "unavailable"

    # ---- FX: Yahoo primary, Frankfurter fallback ----
    for fx in ("USDZAR", "EURZAR", "GBPZAR"):
        sym = YAHOO[fx]
        df = _get_from_yahoo(sym, start, end)
        if not _ensure_df(df):
            ff = _frankfurter_series(base=fx[:3], quote="ZAR",
                                     start_d=start.date(), end_d=end.date())
            if _ensure_df(ff):
                latest = _latest_close(ff)
                prev = _prev_close(ff)
                m_anchor = _last_on_or_before(ff, now - timedelta(days=30))
                y_anchor = _first_on_or_after(ff, now.replace(month=1, day=1))
                data[fx] = _assemble_row(latest, _safe_pct(latest, prev),
                                         _safe_pct(latest, m_anchor), _safe_pct(latest, y_anchor))
                data[fx]["_source"] = "Frankfurter"
                continue
        if _ensure_df(df):
            latest = _latest_close(df)
            prev = _prev_close(df)
            m_anchor = _last_on_or_before(df, now - timedelta(days=30))
            y_anchor = _first_on_or_after(df, now.replace(month=1, day=1))
            data[fx] = _assemble_row(latest, _safe_pct(latest, prev),
                                     _safe_pct(latest, m_anchor), _safe_pct(latest, y_anchor))
            data[fx]["_source"] = f"Yahoo:{sym}"
        else:
            _log(f"No data for {fx} after Yahoo+Frankfurter")
            data[fx] = _assemble_row(None, None, None, None)
            data[fx]["_source"] = "unavailable"

    # ---- Brent / Gold / SP500: Yahoo primary, Stooq backup ----
    for label in ("BRENT", "GOLD", "SP500"):
        sym = YAHOO[label]
        df = _get_from_yahoo(sym, start, end)
        if not _ensure_df(df):
            st_sym = STOOQ.get(label)
            sdf = _stooq_csv(st_sym) if st_sym else None
            if _ensure_df(sdf):
                latest = _latest_close(sdf)
                prev = _prev_close(sdf)
                m_anchor = _last_on_or_before(sdf, now - timedelta(days=30))
                y_anchor = _first_on_or_after(sdf, now.replace(month=1, day=1))
                data[label] = _assemble_row(latest, _safe_pct(latest, prev),
                                            _safe_pct(latest, m_anchor), _safe_pct(latest, y_anchor))
                data[label]["_source"] = f"Stooq:{st_sym}"
                continue
        if _ensure_df(df):
            latest = _latest_close(df)
            prev = _prev_close(df)
            m_anchor = _last_on_or_before(df, now - timedelta(days=30))
            y_anchor = _first_on_or_after(df, now.replace(month=1, day=1))
            data[label] = _assemble_row(latest, _safe_pct(latest, prev),
                                        _safe_pct(latest, m_anchor), _safe_pct(latest, y_anchor))
            data[label]["_source"] = f"Yahoo:{sym}"
        else:
            _log(f"No data for {label} after Yahoo+Stooq")
            data[label] = _assemble_row(None, None, None, None)
            data[label]["_source"] = "unavailable"

    # ---- Bitcoin (ZAR) with 365d cap ----
    btc_today, btc_1d, btc_1m, btc_ytd = _btc_zar_ytd()
    data["BITCOINZAR"] = _assemble_row(btc_today, btc_1d, btc_1m, btc_ytd)
    data["BITCOINZAR"]["_source"] = "CoinGecko"

    # ---- meta ----
    data["timestamp"] = now.strftime("%d %b %Y, %H:%M")
    required = ["JSEALSHARE","USDZAR","EURZAR","GBPZAR","BRENT","GOLD","SP500","BITCOINZAR"]
    data["data_status"] = "complete" if all(k in data for k in required) else "partial"

    # Helpful diagnostics for anchors that couldn't be computed
    for k in required:
        row = data.get(k, {})
        for anchor in ("Change","Monthly","YTD"):
            if row.get(anchor) is None:
                _log(f"Anchor '{anchor}' missing for {k} (source={row.get('_source')})")

    return data
