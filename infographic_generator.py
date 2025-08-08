from PIL import Image, ImageDraw, ImageFont
from datetime import datetime
from config import *

def _format_number(val):
    if val is None:
        return "N/A"
    try:
        return f"{val:,.0f}" if val >= 1000 else f"{val:,.2f}"
    except Exception:
        return "N/A"

def _format_pct(val):
    if val is None:
        return "—"
    try:
        return f"{val:+.1f}%"
    except Exception:
        return "—"

def generate_infographic(data):
    try:
        # Load Georgia fonts with fallback
        georgia = ImageFont.truetype(FONT_PATHS['georgia'], 18)
        georgia_bold = ImageFont.truetype(FONT_PATHS['georgia_bold'], 20)
        footer_font = ImageFont.truetype(FONT_PATHS['georgia'], 16)

        # Canvas
        img = Image.new("RGB", (520, 460), THEME['background'])
        draw = ImageDraw.Draw(img)

        # Header
        header_text = "Market Report"
        timestamp = data.get("timestamp", datetime.now().strftime("%d %b %Y, %H:%M"))
        header_width = georgia_bold.getlength(header_text)
        draw.text(((520 - header_width) // 2, 15), header_text, font=georgia_bold, fill=THEME['header'])
        ts_width = georgia.getlength(timestamp)
        draw.text(((520 - ts_width) // 2, 45), timestamp, font=georgia, fill=THEME['text'])

        # Columns: Metric | Today | 1D | 1M | YTD
        y_position = 90
        x = 20

        # Header row
        col_titles = ["Metric", "Today", "1D", "1M", "YTD"]
        col_widths = [160, 120, 60, 60, 60]
        for i, title in enumerate(col_titles):
            draw.text((x, y_position), title, font=georgia_bold, fill=THEME['text'])
            x += col_widths[i]
        y_position += 30

        # Data rows (order)
        labels = ["JSEALSHARE","USDZAR","EURZAR","GBPZAR","BRENT","GOLD","SP500","BITCOINZAR"]

        for label in labels:
            vals = data.get(label, {})
            today = vals.get("Today")
            d1 = vals.get("Change")
            m1 = vals.get("Monthly")
            ytd = vals.get("YTD")

            # Metric name
            x = 20
            draw.text((x, y_position), label, font=georgia, fill=THEME['text'])
            x += col_widths[0]

            # Today
            draw.text((x, y_position), _format_number(today), font=georgia, fill=THEME['text'])
            x += col_widths[1]

            # 1D, 1M, YTD with colors
            for val in (d1, m1, ytd):
                color = THEME['positive'] if (isinstance(val, (int,float)) and val >= 0) else THEME['negative']
                draw.text((x, y_position), _format_pct(val), font=georgia, fill=color)
                x += col_widths[[2,3,4][0]]
                # Since we don't dynamically track index, just increment properly
                if x == 20 + sum(col_widths[:2]) + col_widths[2]:
                    pass
            # move to next line
            y_position += 32

        # Footer
        footer_text = "Data sources: Yahoo Finance, CoinGecko"
        footer_width = footer_font.getlength(footer_text)
        draw.text(((520 - footer_width) // 2, y_position + 15), footer_text, font=footer_font, fill="#666666")

        filename = f"Market_Report_{datetime.now().strftime('%Y%m%d_%H%M')}.png"
        img.save(filename)
        return filename

    except Exception as e:
        raise RuntimeError(f"Infographic generation failed: {str(e)}")
