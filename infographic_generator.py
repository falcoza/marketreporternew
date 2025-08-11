from __future__ import annotations

from datetime import datetime
from typing import Dict, Any, Optional
from PIL import Image, ImageDraw, ImageFont

# ---------------- Config ----------------
try:
    from config import THEME as _THEME_CFG, FONT_PATHS as _FONT_PATHS_CFG
    THEME = dict(_THEME_CFG)
    FONT_PATHS = dict(_FONT_PATHS_CFG)
except Exception:
    THEME = {
        "background": (255, 255, 255),
        "header": (12, 82, 128),   # blue
        "text": (29, 29, 27),      # near-black
        "positive": (22, 145, 61), # green
        "negative": (200, 30, 35), # red
    }
    FONT_PATHS = {}

# ---------------- Labels / Row order ----------------
LABEL_OVERRIDES = {
    "JSEALSHARE": "JSE All Share",
    "USDZAR": "USD/ZAR",
    "EURZAR": "EUR/ZAR",
    "GBPZAR": "GBP/ZAR",
    "BRENT": "Brent Crude",
    "GOLD": "Gold",
    "SP500": "S&P 500",
    "BITCOINZAR": "Bitcoin ZAR",
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
    p = FONT_PATHS.get(key)
    if p:
        try:
            return ImageFont.truetype(p, size)
        except Exception:
            pass
    candidates = [
        ("Georgia Bold.ttf" if "bold" in key else "Georgia.ttf"),
        ("DejaVuSerif-Bold.ttf" if "bold" in key else "DejaVuSerif.ttf"),
        ("Times New Roman Bold.ttf" if "bold" in key else "Times New Roman.ttf"),
        ("DejaVuSans-Bold.ttf" if "bold" in key else "DejaVuSans.ttf"),
    ]
    for name in candidates:
        try:
            return ImageFont.truetype(name, size)
        except Exception:
            continue
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
    # Match sample: +0.0% is green
    if v >= 0:
        return THEME["positive"]
    return THEME["negative"]

# ---------------- Drawing helpers ----------------
def _text_w(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.FreeTypeFont) -> int:
    return int(draw.textlength(text, font=font))

def _draw_right(draw: ImageDraw.ImageDraw, x_right: int, y: int, text: str, font, fill):
    draw.text((x_right - _text_w(draw, text, font), y), text, font=font, fill=fill)

# ---------------- Main renderer ----------------
def generate_infographic(data: Dict[str, Any], output_path: Optional[str] = None) -> str:
    """
    Renders the report to a PNG and returns the path.
    Title line is "Market Report <timestamp>" using data['timestamp'].
    """
    # Canvas + layout
    W = 520
    TOP_MARGIN = 18
    TITLE_LINE_H = 36
    HEADER_BAND_H = 32
    ROW_H = 36
    FOOTER_H = 40

    rows = [k for k in ROW_ORDER if isinstance(data.get(k), dict)]
    H = TOP_MARGIN + TITLE_LINE_H + HEADER_BAND_H + len(rows) * ROW_H + FOOTER_H

    img = Image.new("RGB", (W, H), THEME["background"])
    draw = ImageDraw.Draw(img)

    # Fonts
    FONT_TITLE = _load_font("georgia_bold", 22)
    FONT_HEAD  = _load_font("georgia_bold", 16)
    FONT_CELL  = _load_font("georgia", 16)
    FONT_FOOT  = _load_font("georgia", 12)

    # Column anchors (tuned to match your sample)
    X_METRIC      = 20
    X_TODAY_RIGHT = 230
    X_1D_RIGHT    = 310
    X_1M_RIGHT    = 390
    X_YTD_RIGHT   = 470

    # ---- Title (single line) ----
    ts = data.get("timestamp", "").strip()
    title = f"Market Report {ts}" if ts else "Market Report"
    draw.text(((W - _text_w(draw, title, FONT_TITLE)) // 2, TOP_MARGIN), title, font=FONT_TITLE, fill=THEME["text"])

    # ---- Table header band ----
    y = TOP_MARGIN + TITLE_LINE_H
    draw.rectangle([(0, y), (W, y + HEADER_BAND_H)], fill=THEME["header"])

    y_text = y + (HEADER_BAND_H - 18) // 2  # vertical centering for ~16–18pt
    draw.text((X_METRIC, y_text), "Metric", font=FONT_HEAD, fill=(255, 255, 255))
    _draw_right(draw, X_TODAY_RIGHT, y_text, "Today", font=FONT_HEAD, fill=(255, 255, 255))
    _draw_right(draw, X_1D_RIGHT,    y_text, "1D%",  font=FONT_HEAD, fill=(255, 255, 255))
    _draw_right(draw, X_1M_RIGHT,    y_text, "1M%",  font=FONT_HEAD, fill=(255, 255, 255))
    _draw_right(draw, X_YTD_RIGHT,   y_text, "YTD%", font=FONT_HEAD, fill=(255, 255, 255))

    # ---- Rows ----
    y = y + HEADER_BAND_H + 10  # small spacing below band
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

    # ---- Footer (centered) ----
    foot = "All values are stated in rands · Data: Yahoo Finance, CoinGecko"
    draw.text(((W - _text_w(draw, foot, FONT_FOOT)) // 2, H - FOOTER_H + 10), foot, font=FONT_FOOT, fill=THEME["text"])

    # Save
    if not output_path:
        output_path = f"Market_Report_{datetime.now().strftime('%Y%m%d_%H%M')}.png"
    img.save(output_path)
    print(f"✅ Generated: {output_path}")
    return output_path
