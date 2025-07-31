import json
import time
from pathlib import Path
from typing import Optional, Dict, Any, Tuple, List

import pytz
import requests
import yfinance as yf
from pycoingecko import CoinGeckoAPI
from datetime import datetime, timezone, timedelta


# ============================== configuration ===============================

SAST = pytz.timezone("Africa/Johannesburg")
CACHE_FILE = Path("last_good_market_data.json")

# Primary Yahoo tickers
TICKERS = {
    "JSE": "^J203.JO",      # JSE All Share Index
    "USDZAR": "ZAR=X",      # USD/ZAR (USD quoted in ZAR)
    "EURZAR": "EURZAR=X",
    "GBPZAR": "GBPZAR=X",
    "BRENT": "BZ=F",        # Brent front-month
    "GOLD": "GC=F",         # COMEX Gold front-month
    "SP500": "^GSPC",       # S&P 500 index
}

# Alternative instruments (same provider group) to keep % math coherent
FALLBACKS = {
    "JSE": ["STXALJ.JO"],   # ALSI ETF proxy on JSE
    "USDZAR": [],           # independent FX fallback handled below
    "EURZAR": [],
    "GBPZAR": [],
    "BRENT": ["CL=F"],      # WTI proxy if Brent fails
    "GOLD": ["XAUUSD=X"],   # spot gold if futures fail
    "SP500": ["SPY"],       # ETF proxy
}

# Retry policy for Yahoo calls
RETRY_ATTEMPTS = 3
RETRY_DELAY = 0.7
RETRY_BACKOFF = 1.8

# Trading-session lookback for the “1M” column (≈ 1 month of trading days)
TRADING_SESSIONS_FOR_MONTH = 22


# ============================== small utilities ==============================

def _retry(n=RETRY_ATTEMPTS, delay=RETRY_DELAY, backoff=RETRY_BACKOFF):
    def deco(fn):
        def wrap(*args, **kwargs):
            d = delay
            last_exc = None
            for _ in range(n):
                try:
                    return fn(*args, **kwargs)
                except Exception as e:
                    last_exc = e
                    time.sleep(d)
                    d *= backoff
            raise last_exc
        return wrap
    return deco


def calculate_percentage(old: Optional[float], new: Optional[float]) -> float:
    """
    % change helper kept backward-compatible for your infographic.
    Returns 0.0 if inputs are missing or old==0.
    """
    if None in (old, new) or old == 0:
        return 0.0
    try:
        return ((new - old) / old) * 100
    except (TypeError, ZeroDivisionError):
        return 0.0


def _save_cache(payload: Dict[str, Any]) -> None:
    try:
        with CACHE_FILE.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def _load_cache() -> Optional[Dict[str, Any]]:
    try:
        if CACHE_FILE.exists():
            with CACHE_FILE.open("r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return None


# ============================== Yahoo helpers ================================

@_retry()
def _yf_history(ticker: str,
                start: Optional[datetime] = None,
                end: Optional[datetime] = None,
                period: Optional[str] = None,
                interval: str = "1d"):
    """
    yfinance history with retries (period OR explicit start/end).
    We do NOT pass a custom session so yfinance can use curl_cffi/httpx.
    """
    tkr = yf.Ticker(ticker)
    if start or end:
        return tkr.history(start=start, end=end, interval=interval, auto_adjust=False)
    return tkr.history(period=period or "7d", interval=interval, auto_adjust=False)


def _yf_latest_price(ticker: str) -> Optional[float]:
    """
    Robust latest price from yfinance. Tries fast_info, info, then history.
    """
    try:
        tkr = yf.Ticker(ticker)

        # 1) fast_info.last_price
        try:
            fi = getattr(tkr, "fast_info", None)
            last = getattr(fi, "last_price", None) if fi else None
            if last is not None:
                return float(last)
        except Exception:
            pass

        # 2) info.regularMarketPrice
        try:
            info = getattr(tkr, "info", {}) or {}
            rmp = info.get("regularMarketPrice")
            if rmp is not None:
                return float(rmp)
        except Exception:
            pass

        # 3) recent history
        try:
            df = _yf_history(ticker, period="5d", interval="1d")
            if not df.empty:
                return float(df["Close"].iloc[-1])
        except Exception:
            pass

        # 4) explicit 10 calendar days window
        utc_now = datetime.now(timezone.utc)
        df = _yf_history(
            ticker,
            start=utc_now - timedelta(days=10),
            end=utc_now + timedelta(days=1),
            interval="1d",
        )
        if not df.empty:
            return float(df["Close"].iloc[-1])

        print(f"⚠️ No Yahoo 'Today' for {ticker} after all attempts.")
        return None
    except Exception as e:
        print(f"⚠️ Price fetch error for {ticker}: {e}")
        return None


def _distinct_trading_closes(df, tz=SAST) -> List[Tuple[datetime, float]]:
    """
    Given a daily price DataFrame, return a list of (local_date, close)
    for distinct trading sessions (no duplicate dates).
    """
    if df.empty:
        return []
    # Ensure timezone awareness
    idx = df.index
    if idx.tz is None:
        df = df.tz_localize("UTC")
    df = df.tz_convert(tz)

    # Group by local date to remove duplicate same-day rows
    closes = []
    for d, grp in df.groupby(df.index.date):
        closes.append((datetime.combine(d, datetime.min.time(), tz), float(grp["Close"].iloc[-1])))
    return closes


def _yf_last_n_trading_closes(ticker: str, n: int) -> List[Tuple[datetime, float]]:
    """
    Fetch enough history to get at least n distinct trading closes.
    Use period and explicit windows as fallbacks.
    """
    # Try a generous period first
    buffer_days = max(40, int(n * 2.5))
    df = _yf_history(ticker, period=f"{buffer_days}d", interval="1d")
    closes = _distinct_trading_closes(df)
    if len(closes) >= n:
        return closes[-n:]

    # Fallback explicit window
    utc_now = datetime.now(timezone.utc)
    df = _yf_history(
        ticker,
        start=utc_now - timedelta(days=buffer_days + 60),
        end=utc_now + timedelta(days=1),
        interval="1d",
    )
    closes = _distinct_trading_closes(df)
    return closes[-n:] if len(closes) >= n else closes


def _yf_trading_ref_prices(ticker: str) -> Tuple[Optional[float], Optional[float]]:
    """
    Returns (prev_trading_close, close_~22_sessions_back).
    Useful for 1D and "1M" columns based on trading sessions.
    """
    arr = _yf_last_n_trading_closes(ticker, TRADING_SESSIONS_FOR_MONTH + 2)
    if len(arr) < 2:
        return None, None
    prev_close = arr[-2][1]
    month_back_idx = max(0, len(arr) - (TRADING_SESSIONS_FOR_MONTH + 1))
    month_back_close = arr[month_back_idx][1] if month_back_idx < len(arr) else None
    return prev_close, month_back_close


def _yf_ytd_first_close(ticker: str, tz=SAST) -> Optional[float]:
    """
    First trading close of current year (for the SAME instrument).
    """
    try:
        now = datetime.now(tz)
        start_date = datetime(now.year, 1, 1, tzinfo=tz)
        end_date = start_date + timedelta(days=40)
        df = _yf_history(
            ticker,
            start=start_date.astimezone(timezone.utc) - timedelta(days=10),
            end=end_date.astimezone(timezone.utc),
            interval="1d",
        )
        closes = _distinct_trading_closes(df, tz)
        # pick first close ON or AFTER Jan 1
        for d, px in closes:
            if d >= start_date:
                return float(px)
        return None
    except Exception as e:
        print(f"⚠️ YTD reference price error for {ticker}: {e}")
        return None


# =========================== FX fallback provider ============================

def _fx_latest_exhost(base: str, quote: str) -> Optional[float]:
    try:
        r = requests.get("https://api.exchangerate.host/latest",
                         params={"base": base, "symbols": quote},
                         timeout=10)
        r.raise_for_status()
        data = r.json()
        return float(data.get("rates", {}).get(quote))
    except Exception as e:
        print(f"⚠️ FX latest fallback error {base}/{quote}: {e}")
        return None


def _fx_on_date_exhost(date: datetime, base: str, quote: str) -> Optional[float]:
    try:
        r = requests.get(f"https://api.exchangerate.host/{date.strftime('%Y-%m-%d')}",
                         params={"base": base, "symbols": quote},
                         timeout=10)
        r.raise_for_status()
        data = r.json()
        return float(data.get("rates", {}).get(quote))
    except Exception as e:
        print(f"⚠️ FX date fallback error {base}/{quote} {date.date()}: {e}")
        return None


def _fx_bundle_consistent(yahoo_ticker: str, base: str, quote: str,
                          use_yahoo_if_possible: bool = True) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    """
    Returns a tuple (today, prev_trading, ~1m_trading, ytd_first) from a single provider chain.
    """
    now = datetime.now(SAST)
    start_of_year = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)

    if use_yahoo_if_possible:
        today = _yf_latest_price(yahoo_ticker)
        if today is not None:
            prev, mback = _yf_trading_ref_prices(yahoo_ticker)
            ytd = _yf_ytd_first_close(yahoo_ticker)
            return today, prev, mback, ytd

    # Independent fallback: exchangerate.host for all refs
    today = _fx_latest_exhost(base, quote)
    # approx previous trading (yesterday UTC) – FX is 24/5; this is an acceptable proxy
    prev = _fx_on_date_exhost(now - timedelta(days=1), base, quote)
    # ~1 month back by calendar (OK for FX)
    mback = _fx_on_date_exhost(now - timedelta(days=30), base, quote)
    ytd = _fx_on_date_exhost(start_of_year, base, quote)
    if today is not None:
        print(f"ℹ️ Using exchangerate.host for {base}/{quote}.")
    return today, prev, mback, ytd


# ============================== Bitcoin helpers ==============================

def fetch_bitcoin_historical(cg: CoinGeckoAPI, days: int) -> Optional[float]:
    try:
        now = datetime.now(timezone.utc)
        target = now - timedelta(days=days)
        window = timedelta(hours=12)
        hist = cg.get_coin_market_chart_range_by_id(
            "bitcoin", "zar",
            int((target - window).timestamp()),
            int((target + window).timestamp())
        )
        prices = hist.get("prices", [])
        if not prices:
            return None
        target_ms = target.timestamp() * 1000
        closest = min(prices, key=lambda x: abs(x[0] - target_ms))
        return float(closest[1])
    except Exception as e:
        print(f"⚠️ Bitcoin historical error ({days}d): {e}")
        return None


def get_bitcoin_ytd_price(cg: CoinGeckoAPI) -> Optional[float]:
    try:
        year = datetime.now(timezone.utc).year
        start = datetime(year, 1, 1, tzinfo=timezone.utc)
        end = start + timedelta(days=1)
        hist = cg.get_coin_market_chart_range_by_id("bitcoin", "zar", int(start.timestamp()), int(end.timestamp()))
        return float(hist["prices"][0][1]) if hist.get("prices") else None
    except Exception as e:
        print(f"⚠️ Bitcoin YTD error: {e}")
        return None


# =============================== main assembly ===============================

def _choose_instrument_for_today(primary: str, fallbacks: Tuple[str, ...]) -> Tuple[Optional[str], Optional[float]]:
    """
    Pick one instrument for the metric and stick to it for all refs.
    """
    p = _yf_latest_price(primary)
    if p is not None:
        return primary, p
    for alt in fallbacks:
        p = _yf_latest_price(alt)
        if p is not None:
            print(f"ℹ️ Using fallback instrument {alt} for {primary}.")
            return alt, p
    return None, None


def _trading_refs_for_instrument(ticker: Optional[str]) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """
    Returns (prev_close, ~1m_back_close, ytd_first_close) for a chosen instrument.
    """
    if not ticker:
        return None, None, None
    prev, mback = _yf_trading_ref_prices(ticker)
    ytd = _yf_ytd_first_close(ticker)
    return prev, mback, ytd


def fetch_market_data() -> Optional[Dict[str, Any]]:
    cg = CoinGeckoAPI()
    now = datetime.now(SAST)

    try:
        # ------------------- JSE (single instrument chain) -------------------
        jse_tkr, jse_today = _choose_instrument_for_today(TICKERS["JSE"], tuple(FALLBACKS["JSE"]))
        jse_prev, jse_mback, jse_ytd = _trading_refs_for_instrument(jse_tkr)

        # ------------------- FX (provider-consistent bundle) -----------------
        usdzar_today, usdzar_prev, usdzar_mback, usdzar_ytd = _fx_bundle_consistent(TICKERS["USDZAR"], "USD", "ZAR")
        eurzar_today, eurzar_prev, eurzar_mback, eurzar_ytd = _fx_bundle_consistent(TICKERS["EURZAR"], "EUR", "ZAR")
        gbpzar_today, gbpzar_prev, gbpzar_mback, gbpzar_ytd = _fx_bundle_consistent(TICKERS["GBPZAR"], "GBP", "ZAR")

        # ------------------- Commodities (stick to one instrument) ----------
        brent_tkr, brent_today = _choose_instrument_for_today(TICKERS["BRENT"], tuple(FALLBACKS["BRENT"]))
        brent_prev, brent_mback, brent_ytd = _trading_refs_for_instrument(brent_tkr)

        gold_tkr, gold_today = _choose_instrument_for_today(TICKERS["GOLD"], tuple(FALLBACKS["GOLD"]))
        gold_prev, gold_mback, gold_ytd = _trading_refs_for_instrument(gold_tkr)

        # ------------------- S&P 500 (index or ETF, but consistent) ---------
        sp_tkr, sp_today = _choose_instrument_for_today(TICKERS["SP500"], tuple(FALLBACKS["SP500"]))
        sp_prev, sp_mback, sp_ytd = _trading_refs_for_instrument(sp_tkr)

        # ------------------- Bitcoin (CoinGecko) ----------------------------
        try:
            btc_now = cg.get_price(ids="bitcoin", vs_currencies="zar")["bitcoin"]["zar"]
        except Exception:
            btc_now = None
        btc_prev = fetch_bitcoin_historical(cg, 1)
        btc_mback = fetch_bitcoin_historical(cg, 30)   # ~month calendar; OK for 24/7 BTC
        btc_ytd = get_bitcoin_ytd_price(cg)

        # ------------------- Assemble payload --------------------------------
        results: Dict[str, Any] = {
            "timestamp": now.strftime("%Y-%m-%d %H:%M"),
            "JSEALSHARE": {
                "Today": jse_today,
                "Change": calculate_percentage(jse_prev, jse_today),
                "Monthly": calculate_percentage(jse_mback, jse_today),
                "YTD": calculate_percentage(jse_ytd, jse_today),
            },
            "USDZAR": {
                "Today": usdzar_today,
                "Change": calculate_percentage(usdzar_prev, usdzar_today),
                "Monthly": calculate_percentage(usdzar_mback, usdzar_today),
                "YTD": calculate_percentage(usdzar_ytd, usdzar_today),
            },
            "EURZAR": {
                "Today": eurzar_today,
                "Change": calculate_percentage(eurzar_prev, eurzar_today),
                "Monthly": calculate_percentage(eurzar_mback, eurzar_today),
                "YTD": calculate_percentage(eurzar_ytd, eurzar_today),
            },
            "GBPZAR": {
                "Today": gbpzar_today,
                "Change": calculate_percentage(gbpzar_prev, gbpzar_today),
                "Monthly": calculate_percentage(gbpzar_mback, gbpzar_today),
                "YTD": calculate_percentage(gbpzar_ytd, gbpzar_today),
            },
            "BRENT": {
                "Today": brent_today,
                "Change": calculate_percentage(brent_prev, brent_today),
                "Monthly": calculate_percentage(brent_mback, brent_today),
                "YTD": calculate_percentage(brent_ytd, brent_today),
            },
            "GOLD": {
                "Today": gold_today,
                "Change": calculate_percentage(gold_prev, gold_today),
                "Monthly": calculate_percentage(gold_mback, gold_today),
                "YTD": calculate_percentage(gold_ytd, gold_today),
            },
            "SP500": {
                "Today": sp_today,
                "Change": calculate_percentage(sp_prev, sp_today),
                "Monthly": calculate_percentage(sp_mback, sp_today),
                "YTD": calculate_percentage(sp_ytd, sp_today),
            },
            "BITCOINZAR": {
                "Today": btc_now,
                "Change": calculate_percentage(btc_prev, btc_now),
                "Monthly": calculate_percentage(btc_mback, btc_now),
                "YTD": calculate_percentage(btc_ytd, btc_now),
            },
        }

        # ------------------- Cache handling ----------------------------------
        # Save last-known-good if core Today values exist; otherwise fill from cache.
        core_ok = all(results[k]["Today"] is not None for k in ["JSEALSHARE", "USDZAR", "EURZAR", "GBPZAR"])
        if core_ok:
            _save_cache(results)
        else:
            cache = _load_cache()
            if cache:
                for key, sec in results.items():
                    if isinstance(sec, dict) and sec.get("Today") is None:
                        cached = cache.get(key, {}).get("Today")
                        if cached is not None:
                            sec["Today"] = cached

        return results

    except Exception as e:
        print(f"❌ Critical error in fetch_market_data: {e}")
        return _load_cache()


# ============================== dev runner ==================================
if __name__ == "__main__":
    data = fetch_market_data()
    if data:
        print("🚀 Market data fetched successfully:")
        print(json.dumps(data, indent=2))
    else:
        print("❌ Failed to fetch market data")
