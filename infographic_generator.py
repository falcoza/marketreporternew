from PIL import Image, ImageDraw, ImageFont
from datetime import datetime
from config import *

def generate_infographic(data):
    try:
        # Load fonts
        georgia = ImageFont.truetype(FONT_PATHS['georgia'], 18)
        georgia_bold = ImageFont.truetype(FONT_PATHS['georgia_bold'], 20)

        # Create image
        img = Image.new("RGB", (450, 580), THEME['background'])
        draw = ImageDraw.Draw(img)

        # Header
        draw.text((10, 10), f"Market Report {data['timestamp']}", 
                 font=georgia_bold, fill=THEME['text'])

        # Table headers
        y = 50
        x = 10
        for col, width in REPORT_COLUMNS:
            draw.rectangle([x, y, x+width, y+30], fill=THEME['header'])
            w = georgia_bold.getlength(col)
            draw.text((x + (width - w)//2, y+5), col, 
                     font=georgia_bold, fill="white")
            x += width

        # Data rows
        y = 80
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

        for name, values in metrics:
            x = 10
            # Metric name
            draw.text((x+5, y+5), name, font=georgia, fill=THEME['text'])
            x += REPORT_COLUMNS[0][1]

            # Values
            for col in ["Today", "Change", "Monthly", "YTD"]:
                value = values[col]
                if col == "Today":
                    text = f"{value:,.2f}" if value < 1000 else f"{value:,.0f}"
                    color = THEME['text']
                else:
                    text = f"{value:+.1f}%"
                    color = THEME['positive'] if value >=0 else THEME['negative']
                
                w = georgia.getlength(text)
                draw.text((x + (REPORT_COLUMNS[1][1] - w)//2, y+5), text, 
                         font=georgia, fill=color)
                x += REPORT_COLUMNS[1][1] if col == "Today" else REPORT_COLUMNS[2][1]
            y += 34

        # Footer
        footer = "Data: Yahoo Finance, CoinGecko · Generated: {datetime.now().strftime('%H:%M')}"
        draw.text((10, 545), footer, font=georgia, fill="#666666")

        filename = f"Market_Report_{datetime.now().strftime('%Y%m%d_%H%M')}.png"
        img.save(filename)
        return filename

    except Exception as e:
        raise RuntimeError(f"Infographic failed: {str(e)}")
