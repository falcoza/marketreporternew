from datetime import datetime, timezone, timedelta
import pytz
import requests
from typing import Optional, Dict, Any, Tuple
from PIL import Image, ImageDraw, ImageFont
from config import *
import time

# API configuration (replace with actual keys in production)
API_KEYS = {
    'YAHOO_FINANCE': 'your_yahoo_api_key',
    'COINGECKO': 'your_coingecko_api_key'
}

def calculate_percentage(old: Optional[float], new: Optional[float]) -> float:
    if None in (old, new) or old == 0:
        return 0.0
    try:
        return ((new - old) / old) * 100
    except (TypeError, ZeroDivisionError):
        return 0.0

def generate_infographic(data: Dict[str, Any]) -> str:
    try:
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

            # Today's Value
            today_val = values["Today"]
            # Formatting based on value magnitude
            if today_val > 1000000:
                today_text = f"{today_val/1000000:.2f}M"
            elif today_val > 1000:
                today_text = f"{today_val:,.0f}"
            else:
                today_text = f"{today_val:,.2f}"
                
            draw.text(
                (x_position + 5, y_position + 5),
                today_text,
                font=georgia,
                fill=THEME['text']
            )
            x_position += REPORT_COLUMNS[1][1]

            # Percentage values
            periods = ["ID%", "IM%", "YTD%"]
            for period in periods:
                value_key = period.replace('%', '')  # Convert "ID%" to "ID"
                value = values[value_key]
                color = THEME['positive'] if value >= 0 else THEME['negative']
                text = f"{value:+.1f}%" if value != 0 else "0.0%"
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
        raise RuntimeError(f"Infographic generation failed: {str(e)}")

def fetch_real_time_data() -> Tuple[float, float, float]:
    """Fetch real-time data from APIs (simplified example)"""
    try:
        # In production, replace with actual API calls:
        # Example for USD/ZAR:
        # response = requests.get(f"https://api.yahoofinance.com/v6/finance/quote?symbols=USDZAR=X&apikey={API_KEYS['YAHOO_FINANCE']}")
        # current_price = response.json()['quoteResponse']['result'][0]['regularMarketPrice']
        # prev_close = response.json()['quoteResponse']['result'][0]['regularMarketPreviousClose']
        
        # For demonstration - returns random fluctuations
        import random
        current_price = 17.89 + random.uniform(-0.5, 0.5)
        prev_close = 17.89
        monthly_open = 17.75
        ytd_open = 18.80
        
        return current_price, prev_close, monthly_open, ytd_open
    except Exception:
        # Fallback values if API fails
        return 17.89, 17.89, 17.75, 18.80

def fetch_market_data() -> Optional[Dict[str, Any]]:
    try:
        # Get real-time data (in production this would be actual API calls)
        jse_current, jse_prev, jse_monthly, jse_ytd = 99674, 99674, 98000, 84500
        usd_current, usd_prev, usd_monthly, usd_ytd = fetch_real_time_data()
        eur_current = 20.71 + (usd_current - 17.89) * 1.15  # Simulate EUR/ZAR movement
        gbp_current = 23.73 + (usd_current - 17.89) * 1.30  # Simulate GBP/ZAR movement
        brent_current = 66.31 + random.uniform(-1, 1) if 'random' in locals() else 66.31
        
        # Calculate percentages dynamically
        def get_percentages(current, prev, monthly_open, ytd_open):
            return {
                "Today": current,
                "ID": calculate_percentage(prev, current),
                "IM": calculate_percentage(monthly_open, current),
                "YTD": calculate_percentage(ytd_open, current)
            }
        
        results = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "JSEALSHARE": get_percentages(jse_current, jse_prev, jse_monthly, jse_ytd),
            "USDZAR": get_percentages(usd_current, usd_prev, usd_monthly, usd_ytd),
            "EURZAR": get_percentages(eur_current, 20.71, 20.40, 19.50),
            "GBPZAR": get_percentages(gbp_current, 23.73, 23.60, 23.55),
            "BRENT": get_percentages(brent_current, 66.31, 67.50, 76.00),
            "GOLD": get_percentages(59961, 60200, 59400, 49700),
            "SP500": get_percentages(6345, 6340, 6250, 5870),
            "BITCOINZAR": get_percentages(2034576, 2040000, 1925000, 1768000)
        }
        return results

    except Exception as e:
        print(f"❌ Critical error in fetch_market_data: {str(e)}")
        return None

if __name__ == "__main__":
    data = fetch_market_data()
    if data:
        try:
            filename = generate_infographic(data)
            print(f"✅ Infographic generated: {filename}")
        except Exception as e:
            print(f"❌ Failed to generate infographic: {str(e)}")
    else:
        print("❌ Failed to fetch market data")
