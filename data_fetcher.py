from __future__ import annotations

from datetime import datetime, timedelta, date
from typing import Dict, Any, Optional, Tuple, List
import io
import time

import pytz
import pandas as pd
import requests
import yfinance as yf
from pycoingecko import CoinGeckoAPI

SAST = pytz.timezone("Africa/Johannesburg")

# ------------ logging ------------
def _log(msg: str) -> None:
    print(f"[data_fetcher] {msg}")

# ------------ generic utils ------------
def _safe_pct(new: Optional[float], base: Optional[float]) -> Optional[float]:
    try:
        if new is None or base is None or base == 0:
            return None
        return (new / base - 1.0) * 100.0
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

def _latest_close(df: pd.DataFrame) -> Optional[float]:
    try:
        return float(df["Close"].dropna().iloc[-1])
    except Exception:
        return None

def _prev_close(df: pd.DataFrame) -> Optional[float]:
    try:
        return float(df["Close"].dropna().iloc[-2])
    except Exception:
        return None

def _first_on_or_after(df: pd.DataFrame, target_dt: datetime) -> Optional[float]:
    try:
        idx = df.index
        for ts in idx:
            tloc = ts.tz_convert(SAST) if ts.tzinfo is not None else SAST.localize(ts)
            if tloc.date() >= target_dt.date():
                return float(df.loc[ts, "Close"])
        return None
    except Exception:
        return None

def _last_on_or_before(df: pd.DataFrame, target_dt: datetime) -> Optional[float]:
    try:
        idx = df.index
        candidate = None
        for ts in idx:
            tloc = ts.tz_convert(SAST) if ts.tzinfo is not None else SAST.localize(ts)
            if tloc.date() <= target_dt.date():
                candidate = float(df.loc[ts, "Close"])
            else:
                break
        return candidate
    except Exception:
        return None

def _assemble_row(today: Optional[float], one_d: Optional[float],
                  one_m: Optional[float], ytd: Optional[float]) -> Dict[str, Optional[float]]:
    return {"Today": today, "Change": one_d, "Monthly": one_m, "YTD": ytd}

# ------------ Yahoo CSV first, then yfinance ------------
def _yahoo_csv(symbol: str, start: datetime, end: datetime) -> Optional[pd.DataFrame]:
    """
    Download daily history via Yahoo CSV endpoint.
    """
    try:
        p1 = int(start.timestamp())
        # Yahoo often excludes the end day unless period2 is end-of-day+ buffer
        p2 = int((end + timedelta(days=1)).timestamp())
        url = (
            f"https://query1.finance.yahoo.com/v7/finance/download/{symbol}"
            f"?period1={p1}&period2={p2}&interval=1d&events=history"
            f"&includeAdjustedClose=true"
        )
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(url, headers=headers, timeout=15)
        if r.status_code != 200 or not r.text or "Date,Open,High,Low,Close" not in r.text:
            _log(f"Yahoo CSV miss ({symbol}) http={r.status_code}")
            return None
        df = pd.read_csv(io.StringIO(r.text))
        if "Date" not in df.columns or "Close" not in df.columns:
            _log(f"Yahoo CSV malformed ({symbol})")
            return None
        df["Date"] = pd.to_datetime(df["Date"], utc=True)
        df = df.set_index("Date").sort_index()
        return df
    except Exception as e:
        _log(f"Yahoo CSV error ({symbol}): {e}")
        return None

def _yahoo_yf(symbol: str, start: datetime, end: datetime) -> Optional[pd.DataFrame]:
    """
    Fallback to yfinance if CSV fails (handles the odd cookie/crumb changes).
    """
    # Try window → period=ytd → period=1y
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
        if _ensure_df(df):
            _log(f"yfinance OK ({symbol}) window")
            return df
        _log(f"yfinance empty ({symbol}) window")
    except Exception as e:
        _log(f"yfinance error ({symbol}) window: {e}")

    for period in ("ytd", "1y"):
        try:
            df = yf.download(symbol, period=period, interval="1d",
                             auto_adjust=False, progress=False, threads=False)
            if _ensure_df(df):
                _log(f"yfinance OK ({symbol}) period={period}")
                return df
            _log(f"yfinance empty ({symbol}) period={period}")
        except Exception as e:
            _log(f"yfinance error ({symbol}) period={period}: {e}")

    return None

def _get_from_yahoo(symbol: str, start: datetime, end: datetime) -> Optional[pd.DataFrame]:
    # CSV first, then yfinance
    df = _yahoo_csv(symbol, start, end)
    if _ensure_df(df):
        _log(f"Yahoo CSV OK ({symbol})")
        return df
    return _yahoo_yf(symbol, start, end)

# ------------ Stooq backups (CSV) ------------
def _stooq_csv(symbol: str) -> Optional[pd.DataFrame]:
    """
    Stooq CSV daily history.
    Examples: CB.F (Brent), GC.F (Gold), ^SPX (S&P 500).
    """
    try:
        # documented by community: /q/d/?s=... (web) and CSV links on the page
        # many mirrors expose CSV via q/d/l/; we try the standard HTML page and parse, but
        # simplest robust route: use a known CSV link pattern used by datareader
        url = f"https://stooq.com/q/d/l/?s={symbol}&i=d"
        r = requests.get(url, timeout=15)
        if r.status_code != 200 or not r.text or "Date,Open,High,Low,Close,Volume" not in r.text:
            _log(f"Stooq CSV miss ({symbol}) http={r.status_code}")
            return None
        df = pd.read_csv(io.StringIO(r.text))
        if "Date" not in df.columns or "Close" not in df.columns:
            _log(f"Stooq CSV malformed ({symbol})")
            return None
        df["Date"] = pd.to_datetime(df["Date"], utc=True)
        df = df.set_index("Date").sort_index()
        return df
    except Exception as e:
        _log(f"Stooq CSV error ({symbol}): {e}")
        return None

# ------------ FX (Frankfurter fallback) ------------
def _frankfurter_series(base: str, quote: str, start_d: date, end_d: date) -> Optional[pd.DataFrame]:
    """
    Returns daily series with 'Close' column named as FX rate base->quote.
    """
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
        return df
    except Exception as e:
        _log(f"Frankfurter error {base}{quote}: {e}")
        return None

# ------------ Bitcoin (CoinGecko) ------------
def _btc_zar_ytd() -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    cg = CoinGeckoAPI()
    now = datetime.now(SAST)
    # Cap at 365 days due to public API limits
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

# ------------ public: fetch all ------------
def fetch_market_data() -> Optional[Dict[str, Any]]:
    now = datetime.now(SAST)
    start = min(now.replace(month=1, day=1), now - timedelta(days=60))
    end = now

    # Verified primary symbols (Yahoo)
    YAHOO = {
        "JSEALSHARE": "^J203.JO",     # FTSE/JSE All Share Index
        "USDZAR": "USDZAR=X",
        "EURZAR": "EURZAR=X",
        "GBPZAR": "GBPZAR=X",
        "BRENT": "BZ=F",              # Brent futures continuous
        "GOLD": "GC=F",               # Gold futures continuous
        "SP500": "^GSPC",             # S&P 500 index
    }
    # Stooq backups where available
    STOOQ = {
        "BRENT": "cb.f",
        "GOLD": "gc.f",
        "SP500": "^spx",
    }

    data: Dict[str, Any] = {}

    # 1) JSE ALSH (Yahoo only)
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
        _log("No data for JSEALSHARE after Yahoo paths")
        data["JSEALSHARE"] = _assemble_row(None, None, None, None)
        data["JSEALSHARE"]["_source"] = "unavailable"

    # 2) FX (Yahoo → Frankfurter)
    for fx in ("USDZAR", "EURZAR", "GBPZAR"):
        sym = YAHOO[fx]
        df = _get_from_yahoo(sym, start, end)
        if not _ensure_df(df):
            # Frankfurter fallback (ECB daily fixes)
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
            y_anchor = _first_o_
