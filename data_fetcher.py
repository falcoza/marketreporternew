from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from pycoingecko import CoinGeckoAPI
import yfinance as yf
import pytz

# --- Percentage Calculator ---
def calculate_percentage(old: Optional[float], new: Optional[float]) -> float:
    if None in (old, new) or old == 0:
        return 0.0
    try:
        return ((new - old) / old) * 100
    except (TypeError, ZeroDivisionError):
        return 0.0

# --- Load Yahoo Ticker Data ---
def fetch_yf_data(ticker: str, ytd_open_fallback: Optional[float] = None) -> Dict[str, float]:
    try:
        tz = pytz.timezone("Africa/Johannesburg")
        now = datetime.now(tz)
        today_str = now.strftime('%Y-%m-%d')
        month_start = now.replace(day=1).strftime('%Y-%m-%d')
        ytd_start = f"{now.year}-01-01"

        df = yf.download(ticker, start=ytd_start, end=today_str, interval='1d', progress=False)

        if df.empty or df.shape[0] < 2:
            raise ValueError(f"No data for {ticker}")

        current = df["Close"][-1]
        prev = df["Close"][-2]
        monthly = df.loc[df.index >= month_start]["Open"].iloc[0] if month_start in df.index.strftime('%Y-%m-%d') else df["Open"][0]
        ytd = df.loc[df.index >= ytd_start]["Open"].iloc[0] if ytd_start in df.index.strftime('%Y-%m-%d') else (ytd_open_fallback or df["Open"][0])

        return {
            "Today": float(current),
            "ID": calculate_percentage(prev, current),
            "IM": calculate_percentage(monthly, current),
            "YTD": calculate_percentage(ytd, current)
        }
    except Exception as e:
        print(f"⚠️ YahooFinance error [{ticker}]: {e}")
        return {"Today": 0.0, "ID": 0.0, "IM": 0.0, "YTD": 0.0}

# --- CoinGecko Bitcoin Fetch ---
def fetch_bitcoin_zar_data() -> Dict[str, float]:
    try:
        cg = CoinGeckoAPI()
        today_data = cg.get_price(ids='bitcoin', vs_currencies='zar')
        today = today_data['bitcoin']['zar']

        # Fetch historical prices
        history = {
            '1d': cg.get_coin_market_chart_range_by_id(
                id='bitcoin',
                vs_currency='zar',
                from_timestamp=(datetime.now() - timedelta(days=2)).timestamp(),
                to_timestamp=datetime.now().timestamp()
            ),
            '1m': cg.get_coin_market_chart_range_by_id(
                id='bitcoin',
                vs_currency='zar',
                from_timestamp=(datetime.now() - timedelta(days=30)).timestamp(),
                to_timestamp=datetime.now().timestamp()
            ),
            'ytd': cg.get_coin_market_chart_range_by_id(
                id='bitcoin',
                vs_currency='zar',
                from_timestamp=datetime(datetime.now().year, 1, 1).timestamp(),
                to_timestamp=datetime.now().timestamp()
            )
        }

        def get_open_price(chart_data):
            return chart_data['prices'][0][1] if chart_data and chart_data.get("prices") else today

        return {
            "Today": today,
            "ID": calculate_percentage(get_open_price(history['1d']), today),
            "IM": calculate_percentage(get_open_price(history['1m']), today),
            "YTD": calculate_percentage(get_open_price(history['ytd']), today),
        }
    except Exception as e:
        print(f"⚠️ CoinGecko error: {e}")
        return {"Today": 0.0, "ID": 0.0, "IM": 0.0, "YTD": 0.0}

# --- Master Fetch ---
def fetch_market_data() -> Optional[Dict[str, Any]]:
    try:
        return {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "JSEALSHARE": fetch_yf_data("^J203.JO", ytd_open_fallback=84500),
            "USDZAR": fetch_yf_data("ZAR=X", ytd_open_fallback=18.80),
            "EURZAR": fetch_yf_data("EURZAR=X", ytd_open_fallback=19.50),
            "GBPZAR": fetch_yf_data("GBPZAR=X", ytd_open_fallback=23.55),
            "BRENT": fetch_yf_data("BZ=F", ytd_open_fallback=76.00),
            "GOLD": fetch_yf_data("GC=F", ytd_open_fallback=49700),
            "SP500": fetch_yf_data("^GSPC", ytd_open_fallback=5870),
            "BITCOINZAR": fetch_bitcoin_zar_data()
        }
    except Exception as e:
        print(f"❌ Critical error in fetch_market_data: {e}")
        return None

# --- Script Entrypoint ---
if __name__ == "__main__":
    data = fetch_market_data()
    if data:
        from generator import generate_infographic  # Ensure you import from correct file
        try:
            filename = generate_infographic(data)
            print(f"✅ Infographic generated: {filename}")
        except Exception as e:
            print(f"❌ Infographic generation failed: {e}")
    else:
        print("❌ Failed to fetch market data")
