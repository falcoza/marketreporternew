from datetime import datetime, timezone, timedelta
import pytz
from typing import Optional, Dict, Any
from PIL import Image, ImageDraw, ImageFont
from config import *


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
        raise RuntimeError(f"Infographic generation failed: {str(e)}")


def fetch_market_data() -> Optional[Dict[str, Any]]:
    try:
        # This would be replaced with actual API calls in production
        results = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "JSEALSHARE": {
                "Today": 99674,
                "Change": 0.0,
                "Monthly": 0.0,
                "YTD": 18.0
            },
            "USDZAR": {
                "Today": 17.89,
                "Change": 0.0,
                "Monthly": 0.0,
                "YTD": -4.9
            },
            "EURZAR": {
                "Today": 20.71,
                "Change": 0.0,
                "Monthly": 0.0,
                "YTD": 6.3
            },
            "GBPZAR": {
                "Today": 23.73,
                "Change": 0.0,
                "Monthly": 0.0,
                "YTD": 0.7
            },
            "BRENT": {
                "Today": 66.31,
                "Change": 0.0,
                "Monthly": 0.0,
                "YTD": -12.7
            },
            "GOLD": {
                "Today": 59961,
                "Change": -0.5,
                "Monthly": 0.9,
                "YTD": 20.7
            },
            "SP500": {
                "Today": 6345,
                "Change": 0.0,
                "Monthly": 0.0,
                "YTD": 8.1
            },
            "BITCOINZAR": {
                "Today": 2034576,
                "Change": -0.2,
                "Monthly": 5.7,
                "YTD": 15.1
            }
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
