from PIL import Image, ImageDraw, ImageFont
from datetime import datetime
from typing import Dict, Any, Optional
import os
from config import THEME, FONT_PATHS, REPORT_COLUMNS

# Display label overrides
LABEL_OVERRIDES = {
    "JSEALSHARE": "JSE ALL SHARE",
}

# Row order for the table (keys in market_data)
ROW_ORDER = [
    "JSEALSHARE",
    "USDZAR",
    "EURZAR",
    "GBPZAR",
    "BRENT",
    "GOLD",
    "SP500",
    "BITCOINZAR",
]

def _load_font(path_key: str, fallback_size: int) -> ImageFont.FreeTypeFont:
    try:
        return ImageFont.truetype(FONT_PATHS[path_key], fallback_size)
    except Exception:
        for guess in ("Georgia.ttf", "Times New Roman.ttf", "DejaVuSerif.ttf"):
            try:
                return ImageFont.truetype(guess, fallback_size)
            except Exception:
                continue
        return ImageFont.load_default()

def _fmt_today(val: Optional[float]) -> str:
    if val is None:
        return "N/A"
    if abs(val) < 100:
        return f"{val:,.2f}"
    return f"{val:,.0f}"

def _fmt_pct(val: Optional[float]) -> str:
    if val is None:
        return "—"
    return f"{val:+.1f}%"

def _cell_color_for_pct(val: Optional[float]) -> tuple:
    if val is None:
        return THEME["text"]
    if val > 0:
        return THEME["positive"]
    if val < 0:
        return THEME["negative"]
    return THEME["text"]

def _row_from_dict(row: Dict[str, Any]) -> list:
    today = _fmt_today(row.get("Today"))
    d1 = _fmt_pct(row.get("Change"))
    m1 = _fmt_pct(row.get("Monthly"))
    ytd = _fmt_pct(row.get("YTD"))
    return [today, d1, m1, ytd]

def generate_infographic(data: Dict[str, Any], output_path: Optional[str] = None) -> str:
    """
    Backward-compatible: if output_path is omitted, we auto-name the file.
    Expects data like:
      data['JSEALSHARE'] = {'Today': 100700.0, 'Change': 0.9, 'Monthly': 3.4, 'YTD': 19.8}
      data['timestamp'] = '08 Aug 2025, 10:35'
    """
    if not output_path:
        ts = datetime.now().strftime("%Y%m%d_%H%M")
        output_path = f"Market_Report_{ts}.png"

    width = 520
    padding = 20
    header_height = 60
    row_height = 40
    footer_height = 50
    col_spacing = 10

    col_widths = [w for _, w in REPORT_COLUMNS]
    col_x = [padding]
    for w in col_widths[:-1]:
        col_x.append(col_x[-1] + w + col_spacing)

    visible_rows = [k for k in ROW_ORDER if isinstance(data.get(k), dict)]
    height = header_height + (len(visible_rows) * row_height) + footer_height

    img = Image.new("RGB", (width, height), THEME["background"])
    draw = ImageDraw.Draw(img)

    font_header   = _load_font("georgia_bold", 20)
    font_row      = _load_font("georgia", 16)
    font_row_bold = _load_font("georgia_bold", 16)
    font_footer   = _load_font("georgia", 12)

    # Header
    title = f"Market Report – {datetime.now().strftime('%d %B %Y')}"
    draw.text((padding, padding), title, font=font_header, fill=THEME["header"])

    ts = data.get("timestamp")
    if isinstance(ts, str):
        draw.text((padding, padding + 26), ts, font=font_row, fill=THEME["text"])

    # Table headers
    y = header_height - 10
    for i, (col_name, _) in enumerate(REPORT_COLUMNS):
        draw.text((col_x[i], y), col_name, font=font_row_bold, fill=THEME["text"])

    # Table rows
    y += 30
    for key in visible_rows:
        rowdict = data.get(key, {})
        label = LABEL_OVERRIDES.get(key, key)
        draw.text((col_x[0], y), label, font=font_row, fill=THEME["text"])

        today_s, d1_s, m1_s, ytd_s = _row_from_dict(rowdict)

        # Today
        draw.text((col_x[1], y), today_s, font=font_row, fill=THEME["text"])
        # 1D
        draw.text((col_x[2], y), d1_s, font=font_row, fill=_cell_color_for_pct(rowdict.get("Change")))
        # 1M
        draw.text((col_x[3], y), m1_s, font=font_row, fill=_cell_color_for_pct(rowdict.get("Monthly")))
        # YTD
        draw.text((col_x[4], y), ytd_s, font=font_row, fill=_cell_color_for_pct(rowdict.get("YTD")))

        y += row_height  # FIXED: this now has a value

    # Footer lines
    footer_text_1 = "Data sourced from Yahoo Finance, CoinGecko and market feeds"
    footer_text_2 = "All values shown in Rands"
    footer_y = height - footer_height + 10
    draw.text((padding, footer_y), footer_text_1, font=font_footer, fill=THEME["text"])
    draw.text((padding, footer_y + 15), footer_text_2, font=font_footer, fill=THEME["text"])

    img.save(output_path)
    print(f"✅ Generated: {output_path}")
    return output_path
