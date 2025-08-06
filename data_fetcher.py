import yfinance as yf
from pycoingecko import CoinGeckoAPI
from datetime import datetime, timezone, timedelta
import pytz
from typing import Optional, Dict, Any
from PIL import Image, ImageDraw, ImageFont
from datetime import datetime
from config import *

def calculate_percentage(old: Optional[float], new: Optional[float]) -> float:
    if None in (old, new) or old == 0:
        return 0.0
def generate_infographic(data):
try:
        return ((new - old) / old) * 100
    except (TypeError, ZeroDivisionError):
        return 0.0

def fetch_historical(ticker: str, days: int) -> Optional[float]:
    try:
        buffer_days = max(20, days * 3)
        stock = yf.Ticker(ticker)
        data = stock.history(period=f"{days + buffer_days}d", interval="1d")
        if data.empty or len(data) < days + 1:
            return None
        return data['Close'].iloc[-days - 1]
    except Exception as e:
        print(f"⚠️ Historical data error for {ticker}: {str(e)}")
        return None

def get_ytd_reference_price(ticker: str) -> Optional[float]:
    try:
        tkr = yf.Ticker(ticker)
        tz = pytz.timezone('Africa/Johannesburg')
        now = datetime.now(tz)
        start_date = tz.localize(datetime(now.year, 1, 1))
        end_date = start_date + timedelta(days=30)
        buffer_start = start_date - timedelta(days=14)
        data = tkr.history(start=buffer_start, end=end_date, interval="1d")
        if not data.empty:
            data.index = data.index.tz_convert(tz)
            ytd_data = data[data.index >= start_date]
            if not ytd_data.empty:
                return ytd_data['Close'].iloc[0]
        return None
    except Exception as e:
        print(f"⚠️ YTD reference price error for {ticker}: {str(e)}")
        return None

def fetch_bitcoin_historical(cg: CoinGeckoAPI, days: int) -> Optional[float]:
    try:
        now = datetime.now(timezone.utc)
        target_date = now - timedelta(days=days)
        window = timedelta(hours=12)
        history = cg.get_coin_market_chart_range_by_id("bitcoin", "zar",
                                                       int((target_date - window).timestamp()),
                                                       int((target_date + window).timestamp()))
        prices = history.get("prices", [])
        if not prices:
            return None
        target_ts = target_date.timestamp() * 1000
        closest_price = min(prices, key=lambda x: abs(x[0] - target_ts))
        return closest_price[1]
    except Exception as e:
        print(f"⚠️ Bitcoin historical data error for {days} days: {str(e)}")
        return None

def get_bitcoin_ytd_price(cg: CoinGeckoAPI) -> Optional[float]:
    try:
        current_year = datetime.now(timezone.utc).year
        start_date = datetime(current_year, 1, 1, tzinfo=timezone.utc)
        end_date = start_date + timedelta(days=1)
        history = cg.get_coin_market_chart_range_by_id("bitcoin", "zar",
                                                       int(start_date.timestamp()),
                                                       int(end_date.timestamp()))
        return history['prices'][0][1] if history.get('prices') else None
    except Exception as e:
        print(f"⚠️ Bitcoin YTD error: {str(e)}")
        return None

def fetch_paxg_historical_price(cg: CoinGeckoAPI, days: int) -> Optional[float]:
    try:
        now = datetime.now(timezone.utc)
        target_date = now - timedelta(days=days)
        window = timedelta(hours=12)
        from_timestamp = int((target_date - window).timestamp())
        to_timestamp = int((target_date + window).timestamp())
        history = cg.get_coin_market_chart_range_by_id("pax-gold", "zar", from_timestamp, to_timestamp)
        prices = history.get("prices", [])
        if not prices:
            return None
        target_ts = target_date.timestamp() * 1000
        closest_price = min(prices, key=lambda x: abs(x[0] - target_ts))
        return closest_price[1]
    except Exception as e:
        print(f"⚠️ PAXG historical fetch error for {days} days: {str(e)}")
        return None

def fetch_paxg_ytd_price(cg: CoinGeckoAPI) -> Optional[float]:
    try:
        current_year = datetime.now(timezone.utc).year
        start_date = datetime(current_year, 1, 1, tzinfo=timezone.utc)
        end_date = start_date + timedelta(days=1)
        history = cg.get_coin_market_chart_range_by_id("pax-gold", "zar",
                                                       int(start_date.timestamp()),
                                                       int(end_date.timestamp()))
        prices = history.get("prices", [])
        return prices[0][1] if prices else None
    except Exception as e:
        print(f"⚠️ PAXG YTD price error: {str(e)}")
        return None

def get_latest_price(ticker: str) -> Optional[float]:
    try:
        stock = yf.Ticker(ticker)
        data = stock.history(period="2d", interval="1d")
        if data.empty:
            return None
        return data['Close'].iloc[-1]
    except Exception as e:
        print(f"⚠️ Price fetch error for {ticker}: {str(e)}")
        return None

def fetch_gold_price_from_coingecko(cg: CoinGeckoAPI) -> Optional[float]:
    try:
        result = cg.get_price(ids="pax-gold", vs_currencies="zar")
        return result.get("pax-gold", {}).get("zar")
    except Exception as e:
        print(f"⚠️ CoinGecko PAXG fetch error: {str(e)}")
        return None

def fetch_jse_historical_yf(days: int) -> Optional[float]:
    ticker = "^J203.JO"
    try:
        buffer_days = max(20, days * 3)
        stock = yf.Ticker(ticker)
        data = stock.history(period=f"{days + buffer_days}d", interval="1d")
        if data.empty or len(data) < days + 1:
            return None
        return data['Close'].iloc[-days - 1]
    except Exception as e:
        print(f"⚠️ JSE YF historical fetch failed: {str(e)}")
        return None

def fetch_market_data() -> Optional[Dict[str, Any]]:
    cg = CoinGeckoAPI()
    tz = pytz.timezone("Africa/Johannesburg")
    now = datetime.now(tz)
    start_of_year = tz.localize(datetime(now.year, 1, 1))

    try:
        jse = get_latest_price("^J203.JO")
        jse_1d = fetch_jse_historical_yf(1)
        jse_30d = fetch_jse_historical_yf(30)
        jse_ytd = fetch_jse_historical_yf((now - start_of_year).days)

        forex = {k: get_latest_price(k) for k in ["ZAR=X", "EURZAR=X", "GBPZAR=X"]}
        commodities = {"BZ=F": get_latest_price("BZ=F")}
        indices = {"^GSPC": get_latest_price("^GSPC")}

        try:
            bitcoin_now = cg.get_price(ids="bitcoin", vs_currencies="zar")["bitcoin"]["zar"]
        except Exception as e:
            print(f"⚠️ Bitcoin current price error: {str(e)}")
            bitcoin_now = None

        gold_now = fetch_gold_price_from_coingecko(cg)

        results = {
            "timestamp": now.strftime("%Y-%m-%d %H:%M"),
            "JSEALSHARE": {
                "Today": jse,
                "Change": calculate_percentage(jse_1d, jse),
                "Monthly": calculate_percentage(jse_30d, jse),
                "YTD": calculate_percentage(jse_ytd, jse)
            },
            "USDZAR": {
                "Today": forex["ZAR=X"],
                "Change": calculate_percentage(fetch_historical("ZAR=X", 1), forex["ZAR=X"]),
                "Monthly": calculate_percentage(fetch_historical("ZAR=X", 30), forex["ZAR=X"]),
                "YTD": calculate_percentage(get_ytd_reference_price("ZAR=X"), forex["ZAR=X"])
            },
            "EURZAR": {
                "Today": forex["EURZAR=X"],
                "Change": calculate_percentage(fetch_historical("EURZAR=X", 1), forex["EURZAR=X"]),
                "Monthly": calculate_percentage(fetch_historical("EURZAR=X", 30), forex["EURZAR=X"]),
                "YTD": calculate_percentage(get_ytd_reference_price("EURZAR=X"), forex["EURZAR=X"])
            },
            "GBPZAR": {
                "Today": forex["GBPZAR=X"],
                "Change": calculate_percentage(fetch_historical("GBPZAR=X", 1), forex["GBPZAR=X"]),
                "Monthly": calculate_percentage(fetch_historical("GBPZAR=X", 30), forex["GBPZAR=X"]),
                "YTD": calculate_percentage(get_ytd_reference_price("GBPZAR=X"), forex["GBPZAR=X"])
            },
            "BRENT": {
                "Today": commodities["BZ=F"],
                "Change": calculate_percentage(fetch_historical("BZ=F", 1), commodities["BZ=F"]),
                "Monthly": calculate_percentage(fetch_historical("BZ=F", 30), commodities["BZ=F"]),
                "YTD": calculate_percentage(get_ytd_reference_price("BZ=F"), commodities["BZ=F"])
            },
            "GOLD": {
                "Today": gold_now,
                "Change": calculate_percentage(fetch_paxg_historical_price(cg, 1), gold_now),
                "Monthly": calculate_percentage(fetch_paxg_historical_price(cg, 30), gold_now),
                "YTD": calculate_percentage(fetch_paxg_ytd_price(cg), gold_now)
            },
            "SP500": {
                "Today": indices["^GSPC"],
                "Change": calculate_percentage(fetch_historical("^GSPC", 1), indices["^GSPC"]),
                "Monthly": calculate_percentage(fetch_historical("^GSPC", 30), indices["^GSPC"]),
                "YTD": calculate_percentage(get_ytd_reference_price("^GSPC"), indices["^GSPC"])
            },
            "BITCOINZAR": {
                "Today": bitcoin_now,
                "Change": calculate_percentage(fetch_bitcoin_historical(cg, 1), bitcoin_now),
                "Monthly": calculate_percentage(fetch_bitcoin_historical(cg, 30), bitcoin_now),
                "YTD": calculate_percentage(get_bitcoin_ytd_price(cg), bitcoin_now)
            }
        }

        return results
        # Load Georgia fonts with fallback
        georgia = ImageFont.truetype(FONT_PATHS['georgia'], 18)
        georgia_bold = ImageFont.truetype(FONT_PATHS['georgia_bold'], 20)
        footer_font = ImageFont.truetype(FONT_PATHS['georgia'], 16)

        # Create canvas (reduced height)
        img = Image.new("RGB", (520, 500), THEME['background'])
        draw = ImageDraw.Draw(img)

        # Header
        header_text = f"Market Report {data['timestamp']}"
        header_width = georgia_bold.getlength(header_text)
        draw.text(
            ((520 - header_width) // 2, 15),
            header_text,
            font=georgia_bold,
            fill=THEME['text']
        )

        # Table Headers
        y_position = 60
        x_position = 25
        for col_name, col_width in REPORT_COLUMNS:
            draw.rectangle(
                [(x_position, y_position), (x_position + col_width, y_position + 30)],
                fill=THEME['header']
            )
            text_width = georgia_bold.getlength(col_name)
            draw.text(
                (x_position + (col_width - text_width) // 2, y_position + 5),
                col_name,
                font=georgia_bold,
                fill="white"
            )
            x_position += col_width

        # Data Rows
        y_position = 90
        metrics = [
            ("JSE All Share", data["JSEALSHARE"]),
            ("USD/ZAR", data["USDZAR"]),
            ("EUR/ZAR", data["EURZAR"]),
            ("GBP/ZAR", data["GBPZAR"]),
            ("Brent Crude", data["BRENT"]),
            ("Gold", data["GOLD"]),
            ("S&P 500", data["SP500"]),
            ("Bitcoin ZAR", data["BITCOINZAR"])
        ]

        for idx, (metric_name, values) in enumerate(metrics):
            x_position = 25
            bg_color = "#F5F5F5" if idx % 2 == 0 else THEME['background']

            draw.rectangle(
                [(25, y_position), (520 - 25, y_position + 34)],
                fill=bg_color
            )

            # Metric Name
            draw.text(
                (x_position + 5, y_position + 5),
                metric_name,
                font=georgia,
                fill=THEME['text']
            )
            x_position += REPORT_COLUMNS[0][1]

            # Today’s Value
            today_val = values["Today"]
            today_text = f"{today_val:,.0f}" if today_val > 1000 else f"{today_val:,.2f}"
            draw.text(
                (x_position + 5, y_position + 5),
                today_text,
                font=georgia,
                fill=THEME['text']
            )
            x_position += REPORT_COLUMNS[1][1]

            # Percentage values
            for period in ["Change", "Monthly", "YTD"]:
                value = values[period]
                color = THEME['positive'] if value >= 0 else THEME['negative']
                text = f"{value:+.1f}%"
                text_width = georgia.getlength(text)
                draw.text(
                    (x_position + (REPORT_COLUMNS[2][1] - text_width) // 2, y_position + 5),
                    text,
                    font=georgia,
                    fill=color
                )
                x_position += REPORT_COLUMNS[2][1]

            y_position += 34

        # Disclaimer (left-aligned)
        disclaimer_text = "All values are stated in rands"
        draw.text(
            (25, y_position + 10),
            disclaimer_text,
            font=footer_font,
            fill="#666666"
        )

        # Footer (bottom-right aligned)
        footer_text = "Data: Yahoo Finance, CoinGecko"
        footer_width = footer_font.getlength(footer_text)
        draw.text(
            (520 - footer_width - 15, y_position + 35),
            footer_text,
            font=footer_font,
            fill="#666666"
        )

        filename = f"Market_Report_{datetime.now().strftime('%Y%m%d_%H%M')}.png"
        img.save(filename)
        return filename

except Exception as e:
        print(f"❌ Critical error in fetch_market_data: {str(e)}")
        return None

if __name__ == "__main__":
    data = fetch_market_data()
    if data:
        print("🚀 Market data fetched successfully:")
        print(data)
    else:
        print("❌ Failed to fetch market data")
        raise RuntimeError(f"Infographic generation failed: {str(e)}")
