from PIL import Image, ImageDraw, ImageFont
from datetime import datetime

# ======================
# CONFIGURATION
# ======================
WIDTH = 520
HEIGHT = 430
BACKGROUND_COLOR = "#FFFFFF"
TEXT_COLOR = "#1D1D1B"
HEADER_COLOR = "#005782"
POSITIVE_COLOR = "#008000"
NEGATIVE_COLOR = "#FF0000"

# Column positions (adjusted for spacing between Metric & Today)
X_METRIC = 30
X_TODAY_RIGHT = 230  # was 215, increased for better spacing
X_1D_RIGHT = 310
X_1M_RIGHT = 390
X_YTD_RIGHT = 470

# Fonts
FONT_PATH_REGULAR = "fonts/Georgia.ttf"
FONT_PATH_BOLD = "fonts/GeorgiaBold.ttf"
FONT_HEADER = ImageFont.truetype(FONT_PATH_BOLD, 22)
FONT_SUBHEADER = ImageFont.truetype(FONT_PATH_REGULAR, 16)
FONT_TABLE_HEADER = ImageFont.truetype(FONT_PATH_BOLD, 14)
FONT_TABLE = ImageFont.truetype(FONT_PATH_REGULAR, 14)
FONT_CREDIT = ImageFont.truetype(FONT_PATH_REGULAR, 10)

# ======================
# MAIN FUNCTION
# ======================
def generate_infographic(data, output_path):
    img = Image.new("RGB", (WIDTH, HEIGHT), BACKGROUND_COLOR)
    draw = ImageDraw.Draw(img)

    # Header
    header_text = "Market Report"
    w, _ = draw.textsize(header_text, font=FONT_HEADER)
    draw.text(((WIDTH - w) / 2, 20), header_text, fill=HEADER_COLOR, font=FONT_HEADER)

    # Date
    date_text = datetime.now().strftime("%d %b %Y, %H:%M")
    w, _ = draw.textsize(date_text, font=FONT_SUBHEADER)
    draw.text(((WIDTH - w) / 2, 50), date_text, fill=TEXT_COLOR, font=FONT_SUBHEADER)

    # Table Headers
    y_start = 90
    draw.text((X_METRIC, y_start), "Metric", fill=TEXT_COLOR, font=FONT_TABLE_HEADER)
    draw.text((X_TODAY_RIGHT - draw.textsize("Today", font=FONT_TABLE_HEADER)[0], y_start),
              "Today", fill=TEXT_COLOR, font=FONT_TABLE_HEADER)
    draw.text((X_1D_RIGHT - draw.textsize("1D%", font=FONT_TABLE_HEADER)[0], y_start),
              "1D%", fill=TEXT_COLOR, font=FONT_TABLE_HEADER)
    draw.text((X_1M_RIGHT - draw.textsize("1M%", font=FONT_TABLE_HEADER)[0], y_start),
              "1M%", fill=TEXT_COLOR, font=FONT_TABLE_HEADER)
    draw.text((X_YTD_RIGHT - draw.textsize("YTD%", font=FONT_TABLE_HEADER)[0], y_start),
              "YTD%", fill=TEXT_COLOR, font=FONT_TABLE_HEADER)

    # Table Rows
    y = y_start + 30
    for metric, values in data.items():
        # Metric name
        draw.text((X_METRIC, y), metric, fill=TEXT_COLOR, font=FONT_TABLE)

        # Today value
        today_val = f"{values['today']:,}" if isinstance(values['today'], (int, float)) else str(values['today'])
        draw.text((X_TODAY_RIGHT - draw.textsize(today_val, font=FONT_TABLE)[0], y),
                  today_val, fill=TEXT_COLOR, font=FONT_TABLE)

        # % change values
        for idx, key in enumerate(["1D", "1M", "YTD"]):
            val = values.get(key, "—")
            color = TEXT_COLOR
            if isinstance(val, str) and val.startswith("+"):
                color = POSITIVE_COLOR
            elif isinstance(val, str) and val.startswith("-"):
                color = NEGATIVE_COLOR

            x_pos = [X_1D_RIGHT, X_1M_RIGHT, X_YTD_RIGHT][idx]
            draw.text((x_pos - draw.textsize(val, font=FONT_TABLE)[0], y),
                      val, fill=color, font=FONT_TABLE)

        y += 28

    # Credits
    credit_text = "Data sourced from Yahoo Finance, CoinGecko and market feeds"
    w, _ = draw.textsize(credit_text, font=FONT_CREDIT)
    draw.text(((WIDTH - w) / 2, HEIGHT - 35), credit_text, fill=TEXT_COLOR, font=FONT_CREDIT)

    rands_text = "All values shown in Rands"
    w, _ = draw.textsize(rands_text, font=FONT_CREDIT)
    draw.text(((WIDTH - w) / 2, HEIGHT - 20), rands_text, fill=TEXT_COLOR, font=FONT_CREDIT)

    img.save(output_path)
