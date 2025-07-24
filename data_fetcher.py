from typing import Optional, Dict, Any
import yfinance as yf
from pycoingecko import CoinGeckoAPI
from datetime import datetime, timezone, timedelta
import pytz

def calculate_percentage(old: Optional[float], new: Optional[float]) -> float:
    if None in (old, new) or old == 0:
        return 0.0
    try:
        return ((new - old) / old) * 100
    except (TypeError, ZeroDivisionError):
        return 0.0

def fetch_historical(ticker: str, days: int) -> Optional[float]:
    try:
        stock = yf.Ticker(ticker)
        if days == 1:
            df = stock.history(period="3d", interval="1d")
            if len(df) >= 2:
                print(f"[DEBUG] {ticker} 1D history:\n{df.tail(2)}")
                return df['Close'].iloc[-2]
        elif days == 30:
            df = stock.history(period="35d", interval="1d")
            if not df.empty:
                print(f"[DEBUG] {ticker} 1M history:\n{df.head(1)}")
                return df['Close'].iloc[0]
        else:
            buffer_days = max(5, days // 5)
            df = stock.history(period=f"{days + buffer_days}d", interval="1d")
            if len(df) >= days + 1:
                print(f"[DEBUG] {ticker} history for {days}d:\n{df.head(3)}")
                return df['Close'].iloc[-days - 1]
        print(f"[WARN] Insufficient historical data for {ticker} ({days}d)")
        return None
    except Exception as e:
        print(f"⚠️ Historical data error for {ticker} ({days}d): {e}")
        return None

def get_ytd_reference_price(ticker: str) -> Optional[float]:
    try:
        stock = yf.Ticker(ticker)
        tz = pytz.timezone('Africa/Johannesburg')
        now = datetime.now(tz)
        start = tz.localize(datetime(now.year, 1, 1))
        end = start + timedelta(days=30)
        buffer_start = start - timedelta(days=14)
        df = stock.history(start=buffer_start, end=end, interval="1d")
        if not df.empty:
            try:
                df.index = df.index.tz_convert(tz)
            except Exception:
                df.index = df.index.tz_localize('UTC').tz_convert(tz)
            ytd = df[df.index >= start]
            if not ytd.empty:
                print(f"[DEBUG] {ticker} YTD price: {ytd['Close'].iloc[0]}")
                return ytd['Close'].iloc[0]
        print(f"[WARN] Empty YTD data for {ticker}")
        return None
    except Exception as e:
        print(f"⚠️ YTD reference price error for {ticker}: {e}")
        return None

def get_latest_price(ticker: str) -> Optional[float]:
    try:
        stock = yf.Ticker(ticker)
        fast = getattr(stock, "fast_info", {})
        last = fast.get("last_price")
        if last is not None:
            print(f"[DEBUG] {ticker} fast_info price: {last}")
            return last
        info_price = stock.info.get("regularMarketPrice")
        if info_price is not None:
            print(f"[DEBUG] {ticker} info price: {info_price}")
            return info_price
        df = stock.history(period="7d", interval="1d", auto_adjust=False)
        if not df.empty:
            print(f"[DEBUG] {ticker} fallback history price: {df['Close'].iloc[-1]}")
            return df["Close"].iloc[-1]
        print(f"[WARN] No data found in any method for {ticker}")
        return None
    except ValueError as e:
        print(f"⚠️ JSON parse error for {ticker}: {e}. Retrying history...")
        try:
            df = yf.Ticker(ticker).history(period="7d", interval="1d", auto_adjust=False)
            if not df.empty:
                return df["Close"].iloc[-1]
            return None
        except Exception as inner:
            print(f"⚠️ Retry failed for {ticker}: {inner}")
            return None
    except Exception as e:
        print(f"⚠️ Price fetch error for {ticker}: {e}")
        return None

def get_bitcoin_ytd_price(cg: CoinGeckoAPI) -> Optional[float]:
    try:
        year = datetime.now(timezone.utc).year
        start = datetime(year, 1, 1, tzinfo=timezone.utc)
        end = start + timedelta(days=1)
        history = cg.get_coin_market_chart_range_by_id(
            "bitcoin", "zar",
            int(start.timestamp()),
            int(end.timestamp())
        )
        return history['prices'][0][1] if history.get('prices') else None
    except Exception as e:
        print(f"⚠️ Bitcoin YTD error: {e}")
        return None

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
        target_ts = target.timestamp() * 1000
        closest = min(prices, key=lambda x: abs(x[0] - target_ts))
        return closest[1]
    except Exception as e:
        print(f"⚠️ Bitcoin historical data error for {days} days: {e}")
        return None

def fetch_market_data() -> Optional[Dict[str, Any]]:
    cg = CoinGeckoAPI()
    utc_now = datetime.now(timezone.utc)
    sast_time = utc_now.astimezone(timezone(timedelta(hours=2)))
    report_time = sast_time.replace(minute=0)

    try:
        jse_tickers = ["^J203.JO", "J203.JO"]
        jse, jse_ticker = None, None
        for tk in jse_tickers:
            price = get_latest_price(tk)
            if price:
                jse, jse_ticker = price, tk
                break
        if not jse:
            print("⚠️ Could not fetch JSE All Share Index")

        usd_zar   = get_latest_price("ZAR=X")
        eur_zar   = get_latest_price("EURZAR=X")
        gbp_zar   = get_latest_price("GBPZAR=X")
        brent     = get_latest_price("BZ=F")
        gold      = get_latest_price("GC=F")
        sp500     = get_latest_price("^GSPC")
        try:
            bitcoin = cg.get_price(ids="bitcoin", vs_currencies="zar")["bitcoin"]["zar"]
        except Exception as e:
            print(f"⚠️ Bitcoin fetch error: {e}")
            bitcoin = None

        jse_1d    = fetch_historical(jse_ticker, 1) if jse_ticker else None
        usd_1d    = fetch_historical("ZAR=X", 1)
        eur_1d    = fetch_historical("EURZAR=X", 1)
        gbp_1d    = fetch_historical("GBPZAR=X", 1)
        brent_1d  = fetch_historical("BZ=F", 1)
        gold_1d   = fetch_historical("GC=F", 1)
        sp_1d     = fetch_historical("^GSPC", 1)
        btc_1d    = fetch_bitcoin_historical(cg, 1)

        jse_1m    = fetch_historical(jse_ticker, 30) if jse_ticker else None
        usd_1m    = fetch_historical("ZAR=X", 30)
        eur_1m    = fetch_historical("EURZAR=X", 30)
        gbp_1m    = fetch_historical("GBPZAR=X", 30)
        brent_1m  = fetch_historical("BZ=F", 30)
        gold_1m   = fetch_historical("GC=F", 30)
        sp_1m     = fetch_historical("^GSPC", 30)
        btc_1m    = fetch_bitcoin_historical(cg, 30)

        jse_ytd   = get_ytd_reference_price(jse_ticker) if jse_ticker else None
        usd_ytd   = get_ytd_reference_price("ZAR=X")
        eur_ytd   = get_ytd_reference_price("EURZAR=X")
        gbp_ytd   = get_ytd_reference_price("GBPZAR=X")
        brent_ytd = get_ytd_reference_price("BZ=F")
        gold_ytd  = get_ytd_reference_price("GC=F")
        sp_ytd    = get_ytd_reference_price("^GSPC")
        btc_ytd   = get_bitcoin_ytd_price(cg)

        return {
            "timestamp": report_time.strftime("%Y-%m-%d %H:%M"),
            "JSEALSHARE": {"Today": jse, "Change": calculate_percentage(jse_1d, jse), "Monthly": calculate_percentage(jse_1m, jse), "YTD": calculate_percentage(jse_ytd, jse)},
            "USDZAR":     {"Today": usd_zar, "Change": calculate_percentage(usd_1d, usd_zar), "Monthly": calculate_percentage(usd_1m, usd_zar), "YTD": calculate_percentage(usd_ytd, usd_zar)},
            "EURZAR":     {"Today": eur_zar, "Change": calculate_percentage(eur_1d, eur_zar), "Monthly": calculate_percentage(eur_1m, eur_zar), "YTD": calculate_percentage(eur_ytd, eur_zar)},
            "GBPZAR":     {"Today": gbp_zar, "Change": calculate_percentage(gbp_1d, gbp_zar), "Monthly": calculate_percentage(gbp_1m, gbp_zar), "YTD": calculate_percentage(gbp_ytd, gbp_zar)},
            "BRENT":      {"Today": brent, "Change": calculate_percentage(brent_1d, brent), "Monthly": calculate_percentage(brent_1m, brent), "YTD": calculate_percentage(brent_ytd, brent)},
            "GOLD":       {"Today": gold, "Change": calculate_percentage(gold_1d, gold), "Monthly": calculate_percentage(gold_1m, gold), "YTD": calculate_percentage(gold_ytd, gold)},
            "SP500":      {"Today": sp500, "Change": calculate_percentage(sp_1d, sp500), "Monthly": calculate_percentage(sp_1m, sp500), "YTD": calculate_percentage(sp_ytd, sp500)},
            "BITCOINZAR": {"Today": bitcoin, "Change": calculate_percentage(btc_1d, bitcoin), "Monthly": calculate_percentage(btc_1m, bitcoin), "YTD": calculate_percentage(btc_ytd, bitcoin)},
        }
    except Exception as e:
        print(f"❌ Critical error in fetch_market_data: {e}")
        return None
