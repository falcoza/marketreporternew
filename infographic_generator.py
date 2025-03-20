from PIL import Image, ImageDraw, ImageFont
from datetime import datetime
from config import *

def generate_infographic(data):
    # Font configuration
    try:
        georgia = ImageFont.truetype(FONT_PATHS['georgia'], 24)
        georgia_bold = ImageFont.truetype(FONT_PATHS['georgia_bold'], 28)
        georgia_small = ImageFont.truetype(FONT_PATHS['georgia'], 18)
    except:
        raise Exception("Georgia font not installed!")

    # New dimensions to eliminate dead space
    img = Image.new("RGB", (850, 1000), THEME['background'])  # Reduced width
    draw = ImageDraw.Draw(img)

    # Title with source
    title = f"Daily Market Report - {data['timestamp']}"
    source_text = "Data: Yahoo Finance | CoinGecko | Google Finance"
    
    draw.text((50, 20), title, font=georgia_bold, fill=THEME['text'])
    draw.text((50, 950), source_text, font=georgia_small, fill="#666666")

    # Table formatting (adjusted columns)
    cols = [
        ("Metric", 200),
        ("Today", 150), 
        ("% Change", 120),
        ("1M Change", 120),
        ("YTD", 100)
    ]

    # Draw table headers
    y = 80
    x = 50
    for col, width in cols:
        draw.rectangle([x, y, x+width, y+40], fill=THEME['header'])
        draw.text((x+10, y+5), col, font=georgia_bold, fill="white")
        x += width

    # Draw data rows (metrics list matches your sample)
    metrics = [
        ("JSE All Share", data["JSE All Share"]),
        ("Rand/Dollar", data["Rand/Dollar"]),
        ("Rand/Euro", data["Rand/Euro"]),
        ("Rand/GBP", data["Rand/GBP"]),
        ("Brent Crude", data["Brent ($/barrel)"]),
        ("Gold", data["Gold ($/oz)"]),
        ("S&P 500", data["S&P500"]),
        ("Bitcoin", data["Bitcoin (ZAR)"])
    ]

    y = 120
    for metric, values in metrics:
        x = 50
        # Metric name
        draw.text((x+10, y+5), metric, font=georgia, fill=THEME['text'])
        x += 200
        
        # Values
        for col in ["Today", "Change", "Monthly", "YTD"]:
            if col == "Today":
                text = f"{values[col]:,.2f}"
                color = THEME['text']
            else:
                text = f"{values[col]:+.2f}%"
                color = THEME['positive'] if values[col] >=0 else THEME['negative']
            
            draw.text((x+10, y+5), text, font=georgia, fill=color)
            x += cols[cols.index((col, 0))][1]  # Dynamic width

        y += 40

    filename = f"Market_Report_{datetime.now().strftime('%Y-%m-%d_%H-%M')}.png"
    img.save(filename)
    return filename
