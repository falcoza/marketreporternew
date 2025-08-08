from PIL import Image, ImageDraw, ImageFont
from datetime import datetime
from typing import Dict, Any, Optional
from config import THEME, FONT_PATHS

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

# ---------- font helpers ----------
def _load_font(path_key: str, size: int) -> ImageFont.FreeTypeFont:
    try:
        return ImageFont.truetype(FONT_PATHS[path_key], size)
    except Exception:
        for guess in ("Georgia.ttf", "Times New Roman.ttf", "DejaVuSerif.ttf"):
            try:
                return ImageFont.truetype(guess, size)
            except Exception:
                continue
        return ImageFont.load_default()

# ---------- formatting ----------
def _fmt_today(val: Optional[float]) -> str:
    if val is None:
        return "N/A"
    try:
        return f"{val:,.2f}" if abs(val) < 100 else f"{val:,.0f}"
    except Exception:
        return "N/A"

def _fmt_pct(val: Optional[float]) -> str:
    if val is None:
        return "—"
    try:
        return f"{val:+.1f}%"
    except Exception:
        return "—"

def _pct_color(val: Optional[float]) -> tuple:
    if val is None:
        return THEME["text"]
    if val > 0:
        return THEME["positive"]
    if val < 0:
        return THEME["negative"]
    return THEME["text"]

# ---------- drawing helpers ----------
def _text_width(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.FreeTypeFont) -> int:
    return int(draw.textlength(text, font=font))

def _draw_right(draw: ImageDraw.ImageDraw, x_right: int, y: int, text: str, font, fill):
    w = _text_width(draw, text, font)
    draw.text((x_right - w, y), text, font=font, fill=fill)

# ---------- main ----------
def generate_infographic(data: Dict[str, Any], output_path: Optional[str] = None) -> str:
    """
    Expects data like:
      data['JSEALSHARE'] = {'Today': 100700.0, 'Change': 1.1, 'Monthly': 3.6, 'YTD': 19.8}
      data['timestamp']   = '08 Aug 2025, 12:54'
    """
    # Canvas
    W = 520
    BG = THEME["background"]

    # Fonts (sizes tuned to your prior style)
    FONT_TITLE   = _load_font("georgia_bold", 28)
    FONT_SUB     = _load_font("georgia", 16)
    FONT_HEAD    = _load_font("georgia_bold", 18)
    FONT_CELL    = _load_font("georgia", 18)
    FONT_FOOT    = _load_font("georgia", 12)

    # Column layout (fixed, hand-tuned)
    # Metric column has more room; numeric columns are right-aligned to these x_rights.
    X_METRIC = 20
    X_TODAY_RIGHT = 215
    X_1D_RIGHT    = 300
    X_1M_RIGHT    = 380
    X_YTD_RIGHT   = 460

    # Compute dynamic height
    rows = [k for k in ROW_ORDER if isinstance(data.get(k), dict)]
    ROW_H = 40
    HEADER_BLOCK_H = 110  # plenty of room for title + timestamp
    FOOTER_H = 60
    H = HEADER_BLOCK_H + ROW_H * len(rows) + FOOTER_H

    # Create canvas
    from PIL import Image
    img = Image.new("RGB", (W, H), BG)
    draw = ImageDraw.Draw(img)

    # ----- Header (centered) -----
    title = "Market Report"
    t_w = _text_width(draw, title, FONT_TITLE)
    draw.text(((W - t_w)//2, 20), title, font=FONT_TITLE, fill=THEME["header"])

    ts = data.get("timestamp")
    if isinstance(ts, str) and ts.strip():
        ts_w = _text_width(draw, ts, FONT_SUB)
        draw.text(((W - ts_w)//2, 60), ts, font=FONT_SUB, fill=THEME["text"])

    # ----- Table headers -----
    y = 90
    draw.text((X_METRIC, y), "Metric", font=FONT_HEAD, fill=THEME["text"])
    _draw_right(draw, X_TODAY_RIGHT, y, "Today", font=FONT_HEAD, fill=THEME["text"])
    _draw_right(draw, X_1D_RIGHT, y, "1D",    font=FONT_HEAD, fill=THEME["text"])
    _draw_right(draw, X_1M_RIGHT, y, "1M",    font=FONT_HEAD, fill=THEME["text"])
    _draw_right(draw, X_YTD_RIGHT, y, "YTD",  font=FONT_HEAD, fill=THEME["text"])
    y += 28

    # ----- Rows -----
    for key in rows:
        row = data.get(key, {}) or {}
        label = LABEL_OVERRIDES.get(key, key)

        today  = row.get("Today")
        d1     = row.get("Change")
        m1     = row.get("Monthly")
        ytd    = row.get("YTD")

        # Metric label (left aligned)
        draw.text((X_METRIC, y), label, font=FONT_CELL, fill=THEME["text"])
        # Today (right aligned)
        _draw_right(draw, X_TODAY_RIGHT, y, _fmt_today(today), font=FONT_CELL, fill=THEME["text"])
        # 1D
        _draw_right(draw, X_1D_RIGHT, y, _fmt_pct(d1), font=FONT_CELL, fill=_pct_color(d1))
        # 1M
        _draw_right(draw, X_1M_RIGHT, y, _fmt_pct(m1), font=FONT_CELL, fill=_pct_color(m1))
        # YTD
        _draw_right(draw, X_YTD_RIGHT, y, _fmt_pct(ytd), font=FONT_CELL, fill=_pct_color(ytd))

        y += ROW_H

    # ----- Footer (two lines, same style) -----
    foot1 = "Data sourced from Yahoo Finance, CoinGecko and market feeds"
    foot2 = "All values shown in Rands"
    y_foot = H - FOOTER_H + 10
    draw.text((X_METRIC, y_foot),          foot1, font=FONT_FOOT, fill=THEME["text"])
    draw.text((X_METRIC, y_foot + 16),     foot2, font=FONT_FOOT, fill=THEME["text"])

    # Save
    if not output_path:
        output_path = f"Market_Report_{datetime.now().strftime('%Y%m%d_%H%M')}.png"
    img.save(output_path)
    print(f"✅ Generated: {output_path}")
    return output_path
