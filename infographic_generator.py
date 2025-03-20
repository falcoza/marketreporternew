from PIL import Image, ImageDraw, ImageFont
from datetime import datetime
from config import *

def generate_infographic(data):
    # Load fonts with strict validation
    try:
        georgia_reg = ImageFont.truetype(FONT_PATHS['georgia'], 22)
        georgia_bold = ImageFont.truetype(FONT_PATHS['georgia_bold'], 26)
        georgia_small = ImageFont.truetype(FONT_PATHS['georgia'], 18)
    except Exception as e:
        raise RuntimeError(f"Font loading failed: {str(e)}")

    # Set up image with tighter dimensions
    img_width = 450  # Reduced from 900
    img = Image.new("RGB", (img_width, 980), THEME['background'])
    draw = ImageDraw.Draw(img)

    # Header with title and date
    title = f"Daily Market Report - {data['timestamp']}"
    draw.text((50, 25), title, font=georgia_bold, fill=THEME['text'])

    # Table configuration
    columns = [
        ("Metric", 180),
        ("Today", 140),
        ("Daily %", 110),
        ("1M %", 110),
        ("YTD %", 110)
    ]

    # Draw table headers
    y_position = 80
    x_position = 50
    for col_name, col_width in columns:
        draw.rectangle(
            [(x_position, y_position), (x_position + col_width, y_position + 40)],
            fill=THEME['header']
        )
        draw.text(
            (x_position + 10, y_position + 8),
            col_name,
            font=georgia_bold,
            fill="white"
        )
        x_position += col_width

    # Table rows
    y_position += 45
    metrics = [
        ("JSE All Share", data["JSE All Share"]),
        ("Rand/Dollar", data["Rand/Dollar"]),
        ("Rand/Euro", data["Rand/Euro"]),
        ("Rand/GBP", data["Rand/GBP"]),
        ("Brent Crude", data["Brent ($/barrel)"]),
        ("Gold", data["Gold ($/oz)"]),
        ("S&P 500", data["S&P500"]),
        ("Bitcoin ZAR", data["Bitcoin (ZAR)"])
    ]

    for idx, (metric_name, values) in enumerate(metrics):
        # Alternate row colors
        bg_color = "#F5F5F5" if idx % 2 == 0 else THEME['background']
        draw.rectangle(
            [(50, y_position), (img_width - 50, y_position + 40)],
            fill=bg_color
        )

        x = 50
        # Metric Name
        draw.text((x + 10, y_position + 8), metric_name, font=georgia_reg, fill=THEME['text'])
        x += columns[0][1]

        # Today's Value
        today_val = f"{values['Today']:,.2f}" if isinstance(values['Today'], float) else values['Today']
        draw.text((x + 10, y_position + 8), today_val, font=georgia_reg, fill=THEME['text'])
        x += columns[1][1]

        # Percentage Changes
        for period in ['Change', 'Monthly', 'YTD']:
            value = values[period]
            color = THEME['positive'] if value >= 0 else THEME['negative']
            text = f"{value:+.2f}%"
            draw.text((x + 10, y_position + 8), text, font=georgia_reg, fill=color)
            x += columns[2][1]  # All percentage columns same width

        y_position += 40

    # Data source footer
    footer_text = "Data Sources: Yahoo Finance, CoinGecko, Google Finance"
    footer_width = georgia_small.getlength(footer_text)
    draw.text(
        (img_width - footer_width - 20, 940),
        footer_text,
        font=georgia_small,
        fill="#666666"
    )

    filename = f"Market_Report_{datetime.now().strftime('%Y-%m-%d_%H-%M')}.png"
    img.save(filename)
    return filename
