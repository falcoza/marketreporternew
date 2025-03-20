from PIL import Image, ImageDraw, ImageFont
from datetime import datetime
from config import THEME, FONT_PATHS, REPORT_COLUMNS

def load_font(size=14, bold=False):
    """Load Georgia font with fallback to default"""
    try:
        font_path = FONT_PATHS["georgia_bold"] if bold else FONT_PATHS["georgia"]
        return ImageFont.truetype(font_path, size)
    except:
        return ImageFont.load_default()

def format_percentage(value):
    """Format percentage with sign and color"""
    symbol = "+" if value >= 0 else ""
    color = THEME["positive"] if value >= 0 else THEME["negative"]
    return f"{symbol}{value:.2f}%", color

def generate_infographic(data):
    # Create image canvas
    img = Image.new("RGB", (900, 1000), THEME["background"])
    draw = ImageDraw.Draw(img)
    y_position = 40

    # Load fonts
    title_font = load_font(28, bold=True)
    header_font = load_font(18, bold=True)
    text_font = load_font(16)

    # Draw title
    title = f"Daily Market Report - {data['timestamp']}"
    draw.text((50, 20), title, fill=THEME["text"], font=title_font)

    # Table headers
    headers = [col[0] for col in REPORT_COLUMNS]
    col_widths = [col[1] for col in REPORT_COLUMNS]
    
    # Draw header background
    draw.rectangle([(50, 80), (850, 120)], fill=THEME["header"])
    
    # Draw header text
    x = 50
    for header, width in zip(headers, col_widths):
        draw.text((x + 10, 85), header, font=header_font, fill="white")
        x += width

    # Draw rows
    y_position = 120
    metrics = [
        ("JSE All Share", data["JSE All Share"]),
        ("Rand/Dollar", data["Rand/Dollar"]),
        ("Rand/Euro", data["Rand/Euro"]),
        ("Rand/GBP", data["Rand/GBP"]),
        ("Brent ($/barrel)", data["Brent ($/barrel)"]),
        ("Gold ($/oz)", data["Gold ($/oz)"]),
        ("S&P500", data["S&P500"]),
        ("Bitcoin (ZAR)", data["Bitcoin (ZAR)"])
    ]

    for idx, (metric_name, values) in enumerate(metrics):
        # Alternate row colors
        bg_color = "#F5F5F5" if idx % 2 == 0 else THEME["background"]
        draw.rectangle([(50, y_position), (850, y_position + 40)], fill=bg_color)

        x = 50
        # Today's Value
        draw.text((x + 10, y_position + 5), f"{values['Today']:,.2f}", 
                font=text_font, fill=THEME["text"])
        x += col_widths[0]

        # Daily Change
        pct_text, pct_color = format_percentage(values["Change"])
        draw.text((x + 10, y_position + 5), pct_text, 
                font=text_font, fill=pct_color)
        x += col_widths[1]

        # Monthly Change
        pct_text, pct_color = format_percentage(values["Monthly"])
        draw.text((x + 10, y_position + 5), pct_text, 
                font=text_font, fill=pct_color)
        x += col_widths[2]

        # YTD Change
        pct_text, pct_color = format_percentage(values["YTD"])
        draw.text((x + 10, y_position + 5), pct_text, 
                font=text_font, fill=pct_color)

        y_position += 40

    # Save with timestamp
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    filename = f"Market_Report_{timestamp}.png"
    img.save(filename)
    
    return filename
