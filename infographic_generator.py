from __future__ import annotations

from datetime import datetime
from typing import Dict, Any, Optional
from PIL import Image, ImageDraw, ImageFont

# ---------------- Config (try/except must be indented) ----------------
try:
    from config import THEME as _THEME_CFG, FONT_PATHS as _FONT_PATHS_CFG
    THEME = dict(_THEME_CFG)
    FONT_PATHS = dict(_FONT_PATHS_CFG)
except Exception:
    THEME = {
        "background": (255, 255, 255),
        "header": (12, 82, 128),
        "text": (29, 29, 27),
        "positive": (22, 145, 61),
        "negative": (200, 30, 35),
    }
    FONT_PATHS = {}

# ---------------- Labels / Row order ----------------
LABEL_OVERRIDES = {
    "JSEALSHARE": "JSE ALL SHARE",
}

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

# ---------------- Font loader ----------------
def _load_font(key: str, size: int) -> ImageFont.FreeTypeFont:
    # 1) config-provided path
    p = FONT_PATHS.get(key)
    if p:
        try:
            return ImageFont.truetype(p, size)
        except Exception:
            pass

    # 2) common system fonts
    candidates = [
        ("DejaVuSerif-Bold.ttf" if "bold" in key else "DejaVuSerif.ttf"),
        ("Georgia Bold.ttf" if "bold" in key else "Georgia.ttf"),
        ("Times New Roman Bold.ttf" if "bold" in key else "Times New Roman.ttf"),
        ("DejaVuSans-Bold.ttf" if "bold" in key else "DejaVuSans.ttf"),
    ]
    for name in candidates:
        try:
            return ImageFont.truetype(name, size)
        except Exception:
            continue

    # 3) never fail
    return ImageFont.load_default()

# ---------------- Formatting helpers ----------------
def _fmt_today(v: Optional[float]) -> str:
    if v is None:
        return "N/A"
    try:
        return f"{v:,.2f}" if abs(v) < 100 else f"{v:,.0f}"
    except Exception:
        return "N/A"

def _fmt_pct(v: Optional[float]) -> str:
    if v is None:
        return "—"
    try:
        return f"{v:+.1f}%"
    except Exception:
        return "—"

def _pct_color(v: Optional[float]) -> tuple:
    if v is None:
        return THEME["text"]
    if v > 0:
        return THEME["positive"]
    if v < 0:
        return THEME["negative"]
    return THEME["text"]

# ---------------- Drawing helpers ----------------
def _text_w(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.FreeTypeFont) -> int:
    return int(draw.textlength(text, font=font))

def _draw_right(draw: ImageDraw.ImageDraw, x_right: int, y: int, text: str, font, fill):
    draw.text((x_right - _text_w(draw, text, font), y), text, font=font, fill=fill)

# ---------------- Main renderer ----------------
def generate_infographic(data: Dict[str, Any], output_path: Optional[str] = None) -> str:
    """
    Renders the report image.

    Expects:
      data['<KEY>'] = {'Today': float, 'Change': float, 'Monthly': float, 'YTD': float}
      data['timestamp'] = '11 Aug 2025, 06:06'

    Saves a PNG and returns its path.
    """
    # Canvas + layout
    W = 520
    HEADER_BLOCK_H = 110
    ROW_H = 40
    FOOTER_H = 60

    rows = [k for k in ROW_ORDER if isinstance(data.get(k), dict)]
    H = HEADER_BLOCK_H + len(rows) * ROW_H + FOOTER_H

    img = Image.new("RGB", (W, H), THEME["background"])
    draw = ImageDraw.Draw(img)

    # Fonts
    FONT_TITLE = _load_font("georgia_bold", 28)
    FONT_SUB   = _load_font("georgia", 16)
    FONT_HEAD  = _load_font("georgia_bold", 18)
    FONT_CELL  = _load_font("georgia", 18)
    FONT_FOOT  = _load_font("georgia", 12)

    # Column anchors
    X_METRIC      = 20
    X_TODAY_RIGHT = 230
    X_1D_RIGHT    = 310
    X_1M_RIGHT    = 390
    X_YTD_RIGHT   = 470

    # Header
    title = "Market Report"
    draw.text(((W - _text_w(draw, title, FONT_TITLE)) // 2, 20), title, font=FONT_TITLE, fill=THEME["header"])

    ts = data.get("timestamp")
    if isinstance(ts, str) and ts.strip():
        draw.text(((W - _text_w(draw, ts, FONT_SUB)) // 2, 60), ts, font=FONT_SUB, fill=THEME["text"])

    # Table headers
    y = 90
    draw.text((X_METRIC, y), "Metric", font=FONT_HEAD, fill=THEME["text"])
    _draw_right(draw, X_TODAY_RIGHT, y, "Today", font=FONT_HEAD, fill=THEME["text"])
    _draw_right(draw, X_1D_RIGHT,    y, "1D",    font=FONT_HEAD, fill=THEME["text"])
    _draw_right(draw, X_1M_RIGHT,    y, "1M",    font=FONT_HEAD, fill=THEME["text"])
    _draw_right(draw, X_YTD_RIGHT,   y, "YTD",   font=FONT_HEAD, fill=THEME["text"])
    y += 28

    # Rows
    for key in rows:
        row = data.get(key) or {}
        label = LABEL_OVERRIDES.get(key, key)

        today = row.get("Today")
        d1    = row.get("Change")
        m1    = row.get("Monthly")
        ytd   = row.get("YTD")

        draw.text((X_METRIC, y), label, font=FONT_CELL, fill=THEME["text"])
        _draw_right(draw, X_TODAY_RIGHT, y, _fmt_today(today), font=FONT_CELL, fill=THEME["text"])
        _draw_right(draw, X_1D_RIGHT,    y, _fmt_pct(d1),      font=FONT_CELL, fill=_pct_color(d1))
        _draw_right(draw, X_1M_RIGHT,    y, _fmt_pct(m1),      font=FONT_CELL, fill=_pct_color(m1))
        _draw_right(draw, X_YTD_RIGHT,   y, _fmt_pct(ytd),     font=FONT_CELL, fill=_pct_color(ytd))

        y += ROW_H

    # Footer
    foot1 = "Data sourced from Yahoo Finance, CoinGecko and market feeds"
    foot2 = "All values shown in Rands"
    y_foot = H - FOOTER_H + 10
    draw.text((X_METRIC, y_foot),      foot1, font=FONT_FOOT, fill=THEME["text"])
    draw.text((X_METRIC, y_foot + 16), foot2, font=FONT_FOOT, fill=THEME["text"])

    # Save
    if not output_path:
        output_path = f"Market_Report_{datetime.now().strftime('%Y%m%d_%H%M')}.png"
    img.save(output_path)
    print(f"✅ Generated: {output_path}")
    return output_path
