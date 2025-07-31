import json
import time
from pathlib import Path
from typing import Optional, Dict, Any, Tuple

import pytz
import requests
import yfinance as yf
from pycoingecko import CoinGeckoAPI
from datetime import datetime, timezone, timedelta


# ---------------------------- config ----------------------------
CACHE_FILE = Path("last_good_market_data.json")
SAST = pytz.timezone("Africa/Johannesburg")

# Yahoo primary tickers
TICKERS = {
    "JSE": "^J203.JO",           # JSE All Share Index (Yahoo)
    "USDZAR": "ZAR=X",           # USD/ZAR
    "EURZAR": "EURZAR=X",        # EUR/ZAR
    "GBPZAR": "GBPZAR=X",        # GBP/ZAR
    "BRENT": "BZ=F",             # Brent (front-month)
    "GOLD": "GC=F",              # COMEX Gold (front-month)
    "SP500": "^GSPC",            # S&P 500 index
}

# Secondary choices (independent or alternative instruments)
FALLBACKS = {
    "JSE": ["STXALJ.JO"],        # ETF proxy for ALSHARE (if index fails)
    "USDZAR": [],                # independent fallback via exchangerate.host below
    "EURZAR": [],
    "GBPZAR": [],
    "BRENT": ["CL=F"],           # WTI as a proxy if Brent fails
    "GOLD": ["XAUUSD=X"],        # spot gold as proxy if futures fail
    "SP500": ["SPY"],            # ETF proxy
}

# Retry & backoff policy
RETRY_ATTEMPTS = 3
RETRY_DELAY = 0.7
RETRY_BACKOFF = 1.8


# --------------------------- small utils ---------------------------
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
    # Keep signature & default to avoid breaking the infographic
    if None in (old, new) or old == 0:
        return 0.0
    try:
        return ((new - old) / old) * 100
    except (TypeError, ZeroDivisionError):
        return 0.0


def _load_cache() -> Optional[Dict[str, Any]]:
    try:
        if CACHE_FILE.exists():
            with CACHE_FILE.open("r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return None


def _save_cache(data: Dict[str, Any]) -> None:
    try:
        with CACHE_FILE.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


# ----------------------- Yahoo Finance core ---------------------
@_retry()
def _yf_history(ticker: str,
                start: Optional[datetime] = None,
                end: Optional[datetime] = None,
                period: Optional[str] = None,
                interval: str = "1d"):
    """
    yfinance history with retries; uses either explicit start/end or period.
    No custom session is passed so yfinance can select its HTTP backend (curl_cffi/httpx).
    """
    tkr = yf.Ticker(ticker)
    if start or end:
        return tkr.history(start=start, end=end, interval=interval, auto_adjust=False)
    return tkr.history(period=period or "7d", interval=interval, auto_adjust=False)


def _yf_latest_price(ticker: str) -> Optional[float]:
    """Robust 'latest' from yfinance."""
    try:
        tkr = yf.Ticker(ticker)

        # 1) fast_info
        try:
            fi = getattr(tkr, "fast_info", None)
            last = getattr(fi, "last_price", None) if fi else None
            if last is not None:
                return float(last)
        except Exception:
            pass

        # 2) info regularMarketPrice
        try:
            info = getattr(tkr, "info", {}) or {}
            rmp = info.get("regularMarketPrice")
            if rmp is not None:
                return float(rmp)
        except Exception:
            pass

        # 3) history(period='2d')
        try:
            df = _yf_history(ticker, period="2d", interval="1d")
            if not df.empty:
                return float(df["Close"].iloc[-1])
        except Exception:
            pass

        # 4) explicit window last 10 calendar days (UTC end+1)
        utc_now = datetime.now(timezone.utc)
        end = utc_now + timedelta(days=1)
        start = utc_now - timedelta(days=10)
        df = _yf_history(ticker, start=start, end=end, interval="1d")
        if not df.empty:
            return float(df["Close"].iloc[-1])

        print(f"⚠️ No price data for {ticker} after all Yahoo attempts.")
        return None
    except Exception as e:
        print(f"⚠️ Price fetch error for {ticker}: {e}")
        return None


def _yf_historical_close(ticker: str, days: int) -> Optional[float]:
    """
    Close 'days' trading days ago via Yahoo (period first; fallback explicit window).
    """
    try:
        buffer_days = max(20, days * 3)
        df = _yf_history(ticker, period=f"{days + buffer_days}d", interval="1d")
        if not df.empty and len(df) >= days + 1:
            return float(df["Close"].iloc[-days - 1])

        # fallback explicit
        utc_now = datetime.now(timezone.utc)
        end = utc_now + timedelta(days=1)
        start = utc_now - timedelta(days=(days + max(30, days * 4)))
        df = _yf_history(ticker, start=start, end=end, interval="1d")
        if not df.empty and len(df) >= days + 1:
            return float(df["Close"].iloc[-days - 1])

        print(f"⚠️ Historical data empty for {ticker} ({days}d) after Yahoo fallback.")
        return None
    except Exception as e:
        print(f"⚠️ Historical data error for {ticker} ({days}d): {e}")
        return None


def _yf_ytd_first_close(ticker: str, tz=SAST) -> Optional[float]:
    """First trading day's close of the current year using Yahoo."""
    try:
        now = datetime.now(tz)
        start_date = datetime(now.year, 1, 1, tzinfo=tz)
        end_date = start_date + timedelta(days=30)
        buffer_start = start_date - timedelta(days=14)

        df = _yf_history(
            ticker,
            start=buffer_start.astimezone(timezone.utc),
            end=end_date.astimezone(timezone.utc),
            interval="1d",
        )
        if not df.empty:
            df.index = df.index.tz_convert(tz)
            ytd = df[df.index >= start_date]
            if not ytd.empty:
                return float(ytd["Close"].iloc[0])
        return None
    except Exception as e:
        print(f"⚠️ YTD reference price error for {ticker}: {e}")
        return None


# ------------------------- FX fallback (exchangerate.host) -------------------------
def _fx_latest_exhost(base: str, quote: str) -> Optional[float]:
    """
    Latest FX via exchangerate.host (no key).
    Returns price as QUOTE per BASE (e.g., USD/ZAR when base='USD', quote='ZAR').
    """
    try:
        url = "https://api.exchangerate.host/latest"
        r = requests.get(url, params={"base": base, "symbols": quote}, timeout=10)
        r.raise_for_status()
        data = r.json()
        rate = data.get("rates", {}).get(quote)
        return float(rate) if rate is not None else None
    except Exception as e:
        print(f"⚠️ FX latest fallback error {base}/{quote}: {e}")
        return None


def _fx_on_date_exhost(date: datetime, base: str, quote: str) -> Optional[float]:
    """
    FX rate on a specific date via exchangerate.host (uses date endpoint).
    """
    try:
        ds = date.strftime("%Y-%m-%d")
        url = f"https://api.exchangerate.host/{ds}"
        r = requests.get(url, params={"base": base, "symbols": quote}, timeout=10)
        r.raise_for_status()
        data = r.json()
        rate = data.get("rates", {}).get(quote)
        return float(rate) if rate is not None else None
    except Exception as e:
        print(f"⚠️ FX date fallback error {base}/{quote} {date}: {e}")
        return None


# ------------------------------ Domain fetchers -----------------------------------
def _pick_today_and_instrument(primary: str, fallbacks: Tuple[str, ...]) -> Tuple[Optional[str], Optional[float]]:
    """
    Decide which instrument to use for *this metric* based on who returns Today.
    Returns (chosen_ticker, today_price).
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


def _hist_same_instrument(chosen: Optional[str], days: int) -> Optional[float]:
    if not chosen:
        return None
    return _yf_historical_close(chosen, days)


def _ytd_same_instrument(chosen: Optional[str]) -> Optional[float]:
    if not chosen:
        return None
    return _yf_ytd_first_close(chosen)


# -------------------------------- Bitcoin helpers -----------------------------------
def fetch_bitcoin_historical(cg: CoinGeckoAPI, days: int) -> Optional[float]:
    try:
        now = datetime.now(timezone.utc)
        target = now - timedelta(days=days)
        window = timedelta(hours=12)
        history = cg.get_coin_market_chart_range_by_id(
            "bitcoin", "zar",
            int((target - window).timestamp()),
            int((target + window).timestamp())
        )
        prices = history.get("prices", [])
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
        history = cg.get_coin_market_chart_range_by_id("bitcoin", "zar", int(start.timestamp()), int(end.timestamp()))
        return float(history["prices"][0][1]) if history.get("prices") else None
    except Exception as e:
        print(f"⚠️ Bitcoin YTD error: {e}")
        return None


# -------------------------------- main API ---------------------------------------
def fetch_market_data() -> Optional[Dict[str, Any]]:
    cg = CoinGeckoAPI()
    now = datetime.now(SAST)
    start_of_year = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)

    try:
        # ---------- JSE ----------
        jse_ticker, jse_today = _pick_today_and_instrument(TICKERS["JSE"], tuple(FALLBACKS["JSE"]))
        jse_1d = _hist_same_instrument(jse_ticker, 1)
        jse_30d = _hist_same_instrument(jse_ticker, 30)
        jse_ytd = _ytd_same_instrument(jse_ticker)
        if jse_ytd is None and jse_ticker:
            days_from_ytd = (now - start_of_year).days
            jse_ytd = _yf_historical_close(jse_ticker, days_from_ytd)

        # ---------- FX (choose provider per pair; if Yahoo fails, use exchangerate.host for all refs) ----------
        def fx_bundle(yahoo_ticker: str, base: str, quote: str) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
            today_y = _yf_latest_price(yahoo_ticker)
            if today_y is not None:
                # Use Yahoo for all refs if possible (same provider)
                d1 = _yf_historical_close(yahoo_ticker, 1)
                d30 = _yf_historical_close(yahoo_ticker, 30)
                ytdref = _yf_ytd_first_close(yahoo_ticker)
                return today_y, d1, d30, ytdref
            # Fallback: exchangerate.host for everything
            today_e = _fx_latest_exhost(base, quote)
            d1_e = _fx_on_date_exhost(now - timedelta(days=1), base, quote)
            d30_e = _fx_on_date_exhost(now - timedelta(days=30), base, quote)
            ytd_e = _fx_on_date_exhost(start_of_year, base, quote)
            if today_e is not None:
                print(f"ℹ️ Using exchangerate.host for {base}/{quote}.")
            return today_e, d1_e, d30_e, ytd_e

        usdzar_today, usdzar_1d, usdzar_30d, usdzar_ytd = fx_bundle(TICKERS["USDZAR"], "USD", "ZAR")
        eurzar_today, eurzar_1d, eurzar_30d, eurzar_ytd = fx_bundle(TICKERS["EURZAR"], "EUR", "ZAR")
        gbpzar_today, gbpzar_1d, gbpzar_30d, gbpzar_ytd = fx_bundle(TICKERS["GBPZAR"], "GBP", "ZAR")

        # ---------- Commodities ----------
        brent_ticker, brent_today = _pick_today_and_instrument(TICKERS["BRENT"], tuple(FALLBACKS["BRENT"]))
        brent_1d = _hist_same_instrument(brent_ticker, 1)
        brent_30d = _hist_same_instrument(brent_ticker, 30)
        brent_ytd = _ytd_same_instrument(brent_ticker)

        gold_ticker, gold_today = _pick_today_and_instrument(TICKERS["GOLD"], tuple(FALLBACKS["GOLD"]))
        gold_1d = _hist_same_instrument(gold_ticker, 1)
        gold_30d = _hist_same_instrument(gold_ticker, 30)
        gold_ytd = _ytd_same_instrument(gold_ticker)

        # ---------- S&P 500 ----------
        sp_ticker, sp_today = _pick_today_and_instrument(TICKERS["SP500"], tuple(FALLBACKS["SP500"]))
        sp_1d = _hist_same_instrument(sp_ticker, 1)
        sp_30d = _hist_same_instrument(sp_ticker, 30)
        sp_ytd = _ytd_same_instrument(sp_ticker)

        # ---------- Bitcoin (ZAR) ----------
        try:
            btc_now = cg.get_price(ids="bitcoin", vs_currencies="zar")["bitcoin"]["zar"]
        except Exception:
            btc_now = None
        btc_1d = fetch_bitcoin_historical(cg, 1)
        btc_30d = fetch_bitcoin_historical(cg, 30)
        btc_ytd = get_bitcoin_ytd_price(cg)

        # ---------- Assemble ----------
        results: Dict[str, Any] = {
            "timestamp": now.strftime("%Y-%m-%d %H:%M"),
            "JSEALSHARE": {
                "Today": jse_today,
                "Change": calculate_percentage(jse_1d, jse_today),
                "Monthly": calculate_percentage(jse_30d, jse_today),
                "YTD": calculate_percentage(jse_ytd, jse_today),
            },
            "USDZAR": {
                "Today": usdzar_today,
                "Change": calculate_percentage(usdzar_1d, usdzar_today),
                "Monthly": calculate_percentage(usdzar_30d, usdzar_today),
                "YTD": calculate_percentage(usdzar_ytd, usdzar_today),
            },
            "EURZAR": {
                "Today": eurzar_today,
                "Change": calculate_percentage(eurzar_1d, eurzar_today),
                "Monthly": calculate_percentage(eurzar_30d, eurzar_today),
                "YTD": calculate_percentage(eurzar_ytd, eurzar_today),
            },
            "GBPZAR": {
                "Today": gbpzar_today,
                "Change": calculate_percentage(gbpzar_1d, gbpzar_today),
                "Monthly": calculate_percentage(gbpzar_30d, gbpzar_today),
                "YTD": calculate_percentage(gbpzar_ytd, gbpzar_today),
            },
            "BRENT": {
                "Today": brent_today,
                "Change": calculate_percentage(brent_1d, brent_today),
                "Monthly": calculate_percentage(brent_30d, brent_today),
                "YTD": calculate_percentage(brent_ytd, brent_today),
            },
            "GOLD": {
                "Today": gold_today,
                "Change": calculate_percentage(gold_1d, gold_today),
                "Monthly": calculate_percentage(gold_30d, gold_today),
                "YTD": calculate_percentage(gold_ytd, gold_today),
            },
            "SP500": {
                "Today": sp_today,
                "Change": calculate_percentage(sp_1d, sp_today),
                "Monthly": calculate_percentage(sp_30d, sp_today),
                "YTD": calculate_percentage(sp_ytd, sp_today),
            },
            "BITCOINZAR": {
                "Today": btc_now,
                "Change": calculate_percentage(btc_1d, btc_now),
                "Monthly": calculate_percentage(btc_30d, btc_now),
                "YTD": calculate_percentage(btc_ytd, btc_now),
            },
        }

        # save last-known-good if we have “Today” for key lines
        core_ok = all(results[k]["Today"] is not None for k in ["JSEALSHARE", "USDZAR", "EURZAR", "GBPZAR"])
        if core_ok:
            _save_cache(results)
        else:
            # fill “Today” gaps from cache if available
            cache = _load_cache()
            if cache:
                for key, sec in results.items():
                    if isinstance(sec, dict) and "Today" in sec and sec["Today"] is None:
                        cached_val = cache.get(key, {}).get("Today")
                        if cached_val is not None:
                            sec["Today"] = cached_val

        return results

    except Exception as e:
        print(f"❌ Critical error in fetch_market_data: {e}")
        cache = _load_cache()
        return cache


# ------------------------------- dev runner --------------------------------
if __name__ == "__main__":
    data = fetch_market_data()
    if data:
        print("🚀 Market data fetched successfully:")
        print(json.dumps(data, indent=2))
    else:
        print("❌ Failed to fetch market data")
