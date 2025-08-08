from __future__ import annotations

from datetime import datetime, timedelta, date
from typing import Dict, Any, Optional, Tuple, List
import io
import random
import re
import time

import pandas as pd
import pytz
import requests
import yfinance as yf
from pycoingecko import CoinGeckoAPI

# Timezone
SAST = pytz.timezone("Africa/Johannesburg")

# ============== logging ==============
def _log(msg: str) -> None:
    print(f"[data_fetcher] {msg}")

# ============== helpers: scalars/frames ==============
def _as_float(x):
    """Safely coerce a scalar/1-element Series to float (avoids FutureWarnings)."""
    if isinstance(x, (pd.Series, pd.Index)):
        if len(x) == 0:
            return None
        x = x.iloc[0]
    if hasattr(x, "item"):
        try:
            return x.item()
        except Exception:
            pass
    try:
        return float(x)
    except Exception:
        return None

def _prepare_df(df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    """Normalize to Date index (UTC), drop dupes, coerce Close numeric."""
    if df is None or not isinstance(df, pd.DataFrame):
        return df
    try:
        # Normalize headers
        lower = {c.lower(): c for c in df.columns}
        if "date" in lower and lower["date"] != "Date":
            df.rename(columns={lower["date"]: "Date"}, inplace=True)
        if "close" in lower and lower["close"] != "Close":
            df.rename(columns={lower["close"]: "Close"}, inplace=True)

        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], utc=True, errors="coerce")
            df = df.dropna(subset=["Date"]).set_index("Date")

        if "Close" not in df.columns:
            return None

        df = df.sort_index()
        df = df[~df.index.duplicated(keep="last")]
        df["Close"] = pd.to_numeric(df["Close"], errors="coerce")
        df = df.dropna(subset=["Close"])
        return df
    except Exception:
        return df

def _have(df: Optional[pd.DataFrame], n:int) -> bool:
    try:
        return isinstance(df, pd.DataFrame) and "Close" in df.columns and df["Close"].dropna().shape[0] >= n
    except Exception:
        return False

def _latest_close(df: pd.DataFrame, now: datetime) -> Optional[float]:
    """Last close ON/BEFORE 'now' in SAST."""
    try:
        for ts in reversed(df.index):
            tloc = ts.tz_convert(SAST) if ts.tzinfo is not None else SAST.localize(ts)
            if tloc <= now:
                return _as_float(df.loc[ts, "Close"])
        return None
    except Exception:
        return None

def _prev_trading_close(df: pd.DataFrame, now: datetime, days_back:int=1) -> Optional[float]:
    """Close on/before (now - days_back days) in SAST (trading-day aware)."""
    try:
        target = now - timedelta(days=days_back)
        out = None
        for ts in df.index:
            tloc = ts.tz_convert(SAST) if ts.tzinfo is not None else SAST.localize(ts)
            if tloc.date() <= target.date():
                out = _as_float(df.loc[ts, "Close"])
            else:
                break
        return out
    except Exception:
        return None

def _first_on_or_after(df: pd.DataFrame, target_dt: datetime) -> Optional[float]:
    try:
        for ts in df.index:
            tloc = ts.tz_convert(SAST) if ts.tzinfo is not None else SAST.localize(ts)
            if tloc.date() >= target_dt.date():
                return _as_float(df.loc[ts, "Close"])
        return None
    except Exception:
        return None

def _last_on_or_before(df: pd.DataFrame, target_dt: datetime) -> Optional[float]:
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

def _row(today: Optional[float], d1: Optional[float], m1: Optional[float], ytd: Optional[float]) -> Dict[str, Optional[float]]:
    return {"Today": today, "Change": d1, "Monthly": m1, "YTD": ytd}

# ============== Yahoo (CSV first, then yfinance) ==============
_UAS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Mozilla/5.0 (X11; Linux x86_64)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
]

def _yahoo_csv(symbol: str, start: datetime, end: datetime, retries:int=2) -> Optional[pd.DataFrame]:
    for attempt in range(retries):
        try:
            p1 = int(start.timestamp())
            p2 = int((end + timedelta(days=1)).timestamp())
            url = (
                f"https://query1.finance.yahoo.com/v7/finance/download/{symbol}"
                f"?period1={p1}&period2={p2}&interval=1d&events=history&includeAdjustedClose=true"
            )
            headers = {"User-Agent": random.choice(_UAS), "Accept": "text/csv,*/*;q=0.8"}
            r = requests.get(url, headers=headers, timeout=15)
            if r.status_code != 200 or "Date" not in r.text:
                _log(f"Yahoo CSV miss ({symbol}) http={r.status_code}")
                continue
            df = pd.read_csv(io.StringIO(r.text))
            return _prepare_df(df)
        except Exception as e:
            _log(f"Yahoo CSV error ({symbol}) attempt {attempt+1}: {e}")
        time.sleep(0.5 + attempt*0.5)
    return None

def _yahoo_yf(symbol: str, start: datetime, end: datetime) -> Optional[pd.DataFrame]:
    for mode in (
        dict(start=start.date(), end=(end + timedelta(days=1)).date(), period=None),
        dict(start=None, end=None, period="ytd"),
        dict(start=None, end=None, period="1y"),
    ):
        try:
            if mode["period"] is None:
                df = yf.download(symbol, start=mode["start"], end=mode["end"], interval="1d",
                                 auto_adjust=False, progress=False, threads=False)
            else:
                df = yf.download(symbol, period=mode["period"], interval="1d",
                                 auto_adjust=False, progress=False, threads=False)
            df = _prepare_df(df)
            if _have(df, 1):
                _log(f"yfinance OK ({symbol}) {('window' if mode['period'] is None else 'period='+mode['period'])}")
                return df
        except Exception as e:
            _log(f"yfinance error ({symbol}) {mode}: {e}")
    return None

def _get_from_yahoo(symbol: str, start: datetime, end: datetime) -> Optional[pd.DataFrame]:
    df = _yahoo_csv(symbol, start, end)
    if _have(df, 1):
        _log(f"Yahoo CSV OK ({symbol})")
        return df
    return _yahoo_yf(symbol, start, end)

# ============== Backups: Stooq, Frankfurter, FT prev close (for JSE 1D) ==============
def _stooq_csv_try(symbols: List[str]) -> Optional[pd.DataFrame]:
    for s in symbols:
        try:
            url = f"https://stooq.com/q/d/l/?s={s}&i=d"
            r = requests.get(url, timeout=15)
            if r.status_code != 200 or "Date" not in r.text:
                _log(f"Stooq CSV miss ({s}) http={r.status_code}")
                continue
            df = pd.read_csv(io.StringIO(r.text))
            df = _prepare_df(df)
            if _have(df, 1):
                _log(f"Stooq CSV OK ({s})")
                return df
        except Exception as e:
            _log(f"Stooq CSV error ({s}): {e}")
    return None

def _frankfurter_series(base: str, quote: str, start_d: date, end_d: date) -> Optional[pd.DataFrame]:
    try:
        url = f"https://api.frankfurter.dev/v1/{start_d.isoformat()}..{end_d.isoformat()}?base={base}&symbols={quote}"
        r = requests.get(url, timeout=15)
        if r.status_code != 200:
            _log(f"Frankfurter http={r.status_code} {base}{quote}")
            return None
        rates = r.json().get("rates", {})
        if not rates:
            return None
        rows = []
        for dstr, m in rates.items():
            if quote in m:
                rows.append((pd.to_datetime(dstr, utc=True), float(m[quote])))
        if not rows:
            return None
        df = pd.DataFrame(rows, columns=["Date", "Close"]).set_index("Date").sort_index()
        return _prepare_df(df)
    except Exception as e:
        _log(f"Frankfurter error {base}{quote}: {e}")
        return None

def _ft_prev_close_jalsh() -> Optional[float]:
    """FT JALSH:JNB 'Previous close' — backup for JSE 1D when Yahoo looks wrong."""
    try:
        url = "https://markets.ft.com/data/indices/tearsheet/summary?s=JALSH:JNB"
        r = requests.get(url, timeout=15, headers={"User-Agent": random.choice(_UAS)})
        if r.status_code != 200:
            _log(f"FT JALSH http={r.status_code}")
            return None
        html = r.text
        m = re.search(r'"previousClose"\s*:\s*([0-9.,]+)', html)
        if not m:
            m = re.search(r'Previous close[^0-9]*([0-9][0-9,.\s]+)</', html, flags=re.I)
        if not m:
            _log("FT JALSH: previous close not found")
            return None
        return float(m.group(1).replace(",", "").strip())
    except Exception as e:
        _log(f"FT JALSH parse error: {e}")
        return None

# ============== Bitcoin (≤ 365 days) ==============
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
            _log(f"CoinGecko spot error {attempt+1}: {e}")
            time.sleep(1 + attempt)

    # history
    prices = None
    for attempt in range(3):
        try:
            hist = cg.get_coin_market_chart_by_id(id="bitcoin", vs_currency="zar", days=days)
            prices = hist.get("prices") if hist else None
            if prices:
                break
        except Exception as e:
            _log(f"CoinGecko history error {attempt+1}: {e}")
            time.sleep(1 + attempt)

    if not prices:
        return spot, None, None, None

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

# ============== YTD anchor (final) ==============
def _ytd_anchor_value(df: pd.DataFrame, now: datetime) -> Optional[float]:
    """
    YTD base that matches Yahoo:
      1) Find the last trading close ON/BEFORE Dec 31 (prior year),
         walking back up to 10 trading days into December if needed.
      2) Only if none exist, fall back to first trading close ON/AFTER Jan 1.
    """
    # Step 1: prior-year close (Dec 31 or earlier)
    for back in range(0, 11):  # 0..10 days back
        dt = datetime(now.year - 1, 12, 31, 23, 59, tzinfo=SAST) - timedelta(days=back)
        base = _last_on_or_before(df, dt)
        if base is not None:
            return base
    # Step 2: fallback to first close of this year
    jan1 = datetime(now.year, 1, 1, 0, 0, tzinfo=SAST)
    return _first_on_or_after(df, jan1)

# ============== Public: fetch everything ==============
def fetch_market_data() -> Optional[Dict[str, Any]]:
    now = datetime.now(SAST)

    # Start mid-December prior year so the Dec 31 bar is always in range
    start = datetime(now.year - 1, 12, 15, 0, 0, tzinfo=SAST)
    end = now

    # Verified Yahoo symbols / candidates
    YAHOO = {
        "JSEALSHARE": "^J203.JO",
        "USDZAR": "USDZAR=X",
        "EURZAR": "EURZAR=X",
        "GBPZAR": "GBPZAR=X",
        "BRENT": ["BZ=F", "BRN=F", "CO=F"],
        "GOLD":  ["GC=F", "XAUUSD=X"],
        "SP500": ["^GSPC", "^SPX", "SPY"],
    }
    STOOQ = {"BRENT": ["cb.f", "co.f", "brent"], "GOLD": ["gc.f", "xauusd"], "SP500": ["^spx"]}

    data: Dict[str, Any] = {}

    # ---- JSE ALSH ----
    jse_df = _get_from_yahoo(YAHOO["JSEALSHARE"], start, end)
    if _have(jse_df, 1):
        today = _latest_close(jse_df, now)

        # 1D: yesterday → try 2d → FT prev close rescue → null if implausible
        prev = _prev_trading_close(jse_df, now, days_back=1) or _prev_trading_close(jse_df, now, days_back=2)
        one_d = _safe_pct(today, prev)
        if one_d is not None and abs(one_d) > 12:
            prev2 = _prev_trading_close(jse_df, now, days_back=2)
            alt = _safe_pct(today, prev2)
            if alt is not None and abs(alt) <= 12:
                one_d = alt
            else:
                ft_prev = _ft_prev_close_jalsh()
                alt2 = _safe_pct(today, ft_prev) if ft_prev else None
                one_d = alt2 if (alt2 is not None and abs(alt2) <= 12) else None

        # 1M / YTD
        m_anchor = _last_on_or_before(jse_df, now - timedelta(days=30))
        y_anchor = _ytd_anchor_value(jse_df, now)            # <-- matches Yahoo YTD

        data["JSEALSHARE"] = _row(today, one_d, _safe_pct(today, m_anchor), _safe_pct(today, y_anchor))
        data["JSEALSHARE"]["_source"] = "Yahoo:^J203.JO"
    else:
        data["JSEALSHARE"] = _row(None, None, None, None)
        data["JSEALSHARE"]["_source"] = "unavailable"

    # ---- FX: Yahoo → Frankfurter ----
    for fx in ("USDZAR", "EURZAR", "GBPZAR"):
        sym = YAHOO[fx]
        df = _get_from_yahoo(sym, start, end)
        if not _have(df, 1):
            ff = _frankfurter_series(base=fx[:3], quote="ZAR", start_d=start.date(), end_d=end.date())
            if _have(ff, 1):
                df = ff
                source = "Frankfurter"
            else:
                data[fx] = _row(None, None, None, None); data[fx]["_source"] = "unavailable"; continue
        else:
            source = f"Yahoo:{sym}"

        today = _latest_close(df, now)
        prev = _prev_trading_close(df, now, days_back=1) or _prev_trading_close(df, now, days_back=2)
        m_anchor = _last_on_or_before(df, now - timedelta(days=30))
        y_anchor = _ytd_anchor_value(df, now)
        data[fx] = _row(today, _safe_pct(today, prev), _safe_pct(today, m_anchor), _safe_pct(today, y_anchor))
        data[fx]["_source"] = source

    # ---- Brent / Gold / SP500: Yahoo multi-symbol → Stooq multi-symbol ----
    def _fetch_multi_yahoo(symbols: List[str]) -> Optional[pd.DataFrame]:
        for s in symbols:
            df = _get_from_yahoo(s, start, end)
            if _have(df, 1):
                return df
        return None

    for label in ("BRENT", "GOLD", "SP500"):
        df = _fetch_multi_yahoo(YAHOO[label] if isinstance(YAHOO[label], list) else [YAHOO[label]])
        source = None
        if not _have(df, 1):
            sdf = _stooq_csv_try(STOOQ.get(label, []))
            if _have(sdf, 1):
                df, source = sdf, "Stooq"
        else:
            used = (YAHOO[label][0] if isinstance(YAHOO[label], list) else YAHOO[label])
            source = f"Yahoo:{used}"

        if _have(df, 1):
            today = _latest_close(df, now)
            prev = _prev_trading_close(df, now, days_back=1) or _prev_trading_close(df, now, days_back=2)
            m_anchor = _last_on_or_before(df, now - timedelta(days=30))
            y_anchor = _ytd_anchor_value(df, now)
            data[label] = _row(today, _safe_pct(today, prev), _safe_pct(today, m_anchor), _safe_pct(today, y_anchor))
            data[label]["_source"] = source
        else:
            data[label] = _row(None, None, None, None); data[label]["_source"] = "unavailable"

    # ---- Bitcoin (ZAR) ----
    btc_today, btc_1d, btc_1m, btc_ytd = _btc_zar_ytd()
    data["BITCOINZAR"] = _row(btc_today, btc_1d, btc_1m, btc_ytd)
    data["BITCOINZAR"]["_source"] = "CoinGecko"

    # ---- meta & diagnostics ----
    data["timestamp"] = now.strftime("%d %b %Y, %H:%M")
    required = ["JSEALSHARE","USDZAR","EURZAR","GBPZAR","BRENT","GOLD","SP500","BITCOINZAR"]
    data["data_status"] = "complete" if all(k in data for k in required) else "partial"

    # optional: log missing anchors (kept — handy in CI logs)
    for k in required:
        row = data.get(k, {})
        for anchor in ("Change","Monthly","YTD"):
            if row.get(anchor) is None:
                _log(f"Anchor '{anchor}' missing for {k} (source={row.get('_source')})")

    return data
