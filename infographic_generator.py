from PIL import Image, ImageDraw, ImageFont
from datetime import datetime
from config import *

def generate_infographic(data):
    # Load fonts with verification
    try:
        georgia = ImageFont.truetype(FONT_PATHS['georgia'], 18)
        georgia_bold = ImageFont.truetype(FONT_PATHS['georgia_bold'], 20)
    except Exception as e:
        raise RuntimeError(f"Font load failed: {str(e)}\nVerify font install in workflow!")

    # Create 450px wide image
    img = Image.new("RGB", (450, 650), THEME['background'])
    draw = ImageDraw.Draw(img)

    # Header
    draw.text((10, 10), f"Market Report {data['timestamp']}", 
             font=georgia_bold, fill=THEME['text'])

    # Table headers
    y = 50
    x = 10
    for col, width in REPORT_COLUMNS:
        draw.rectangle([x, y, x+width, y+30], fill=THEME['header'])
        draw.text((x+5, y+5), col, font=georgia_bold, fill="white")
        x += width

    # Data rows
    y = 80
    metrics = [
        ("JSE All Share", data["JSE All Share"]),
        ("ZAR/USD", data["Rand/Dollar"]),
        ("ZAR/EUR", data["Rand/Euro"]),
        ("ZAR/GBP", data["Rand/GBP"]),
        ("Brent", data["Brent ($/barrel)"]),
        ("Gold", data["Gold ($/oz)"]),
        ("S&P 500", data["S&P500"]),
        ("Bitcoin", data["Bitcoin (ZAR)"])
    ]

    for idx, (name, values) in enumerate(metrics):
        x = 10
        # Metric name
        draw.text((x+5, y+5), name, font=georgia, fill=THEME['text'])
        x += REPORT_COLUMNS[0][1]

        # Values
        for col in ["Today", "Change", "Monthly", "YTD"]:
            value = values[col]
            if col == "Today":
                text = f"{value:,.2f}" if isinstance(value, float) else str(value)
                color = THEME['text']
            else:
                text = f"{value:+.1f}%"
                color = THEME['positive'] if value >= 0 else THEME['negative']
            
            draw.text((x+5, y+5), text, font=georgia, fill=color)
            x += REPORT_COLUMNS[1][1] if col == "Today" else REPORT_COLUMNS[2][1]

        y += 35

    # Footer
    footer = "Data: Yahoo Finance, CoinGecko"
    draw.text((10, 620), footer, font=georgia, fill="#666666")

    filename = f"report_{datetime.now().strftime('%Y%m%d_%H%M')}.png"
    img.save(filename)
    return filename
