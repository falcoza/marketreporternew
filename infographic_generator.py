from PIL import Image, ImageDraw, ImageFont
from datetime import datetime
from config import THEME, FONT_PATHS, REPORT_COLUMNS

# Display label overrides (key in your data dict -> label on the image)
LABEL_OVERRIDES = {
    "JSEALSHARE": "JSE ALL SHARE",
}

def generate_infographic(data: dict, output_path: str):
    """
    Expects `data` shaped like:
      {
        "JSEALSHARE": ["100,768", "+0.9%", "+3.4%", "+19.8%"],
        "USDZAR":     ["17.98",  "-0.2%", "+1.1%",  "+3.5%"],
        ...
      }
    Column layout is driven by config.REPORT_COLUMNS, e.g.:
      REPORT_COLUMNS = [("Metric", 180), ("Today", 90), ("1D", 70), ("1M", 70), ("YTD", 70)]
    """

    # ---- Canvas sizing ----
    width = 520
    padding = 20
    header_height = 60
    row_height = 40
    footer_height = 50
    col_spacing = 10

    col_widths = [col[1] for col in REPORT_COLUMNS]
    col_x_positions = [padding]
    for w in col_widths[:-1]:
        col_x_positions.append(col_x_positions[-1] + w + col_spacing)

    # Only count the real rows (ignore any metadata if present)
    row_count = sum(1 for k, _ in data.items() if isinstance(k, str))
    height = header_height + (row_count * row_height) + footer_height

    img = Image.new("RGB", (width, height), THEME["background"])
    draw = ImageDraw.Draw(img)

    # ---- Fonts ----
    font_header   = ImageFont.truetype(FONT_PATHS["georgia_bold"], 20)
    font_row      = ImageFont.truetype(FONT_PATHS["georgia"], 16)
    font_row_bold = ImageFont.truetype(FONT_PATHS["georgia_bold"], 16)
    font_footer   = ImageFont.truetype(FONT_PATHS["georgia"], 12)

    # ---- Header ----
    title = f"Market Report – {datetime.now().strftime('%d %B %Y')}"
    draw.text((padding, padding), title, font=font_header, fill=THEME["header"])

    # ---- Table headers ----
    y = header_height - 10
    for i, (col_name, _) in enumerate(REPORT_COLUMNS):
        draw.text((col_x_positions[i], y), col_name, font=font_row_bold, fill=THEME["text"])

    # ---- Table rows ----
    y += 30
    for metric, values in data.items():
        if not isinstance(values, (list, tuple)):
            continue  # skip metadata like timestamp if present

        # Apply display label override where needed
        display_metric = LABEL_OVERRIDES.get(metric, metric)

        # Metric label
        draw.text((col_x_positions[0], y), display_metric, font=font_row, fill=THEME["text"])

        # Cells
        for i, val in enumerate(values):
            fill_color = THEME["text"]
            if isinstance(val, str) and val.strip().endswith("%"):
                try:
                    num = float(val.strip().replace("%", "").replace("+", ""))
                    if num > 0:
                        fill_color = THEME["positive"]
                    elif num < 0:
                        fill_color = THEME["negative"]
                except ValueError:
                    pass
            draw.text((col_x_positions[i + 1], y), str(val), font=font_row, fill=fill_color)

        y += row_height

    # ---- Footer credits (two lines; same font/colour/style) ----
    footer_text_1 = "Data sourced from Yahoo Finance, CoinGecko and market feeds"
    footer_text_2 = "All values shown in Rands"
    footer_y = height - footer_height + 10
    draw.text((padding, footer_y), footer_text_1, font=font_footer, fill=THEME["text"])
    draw.text((padding, footer_y + 15), footer_text_2, font=font_footer, fill=THEME["text"])

    # ---- Save ----
    img.save(output_path)
    print(f"✅ Generated: {output_path}")
