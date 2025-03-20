from PIL import Image, ImageDraw, ImageFont
import textwrap

def load_font(size, bold=False):
    try:
        font_type = "georgia_bold" if bold else "georgia"
        return ImageFont.truetype(FONT_PATHS[font_type], size)
    except:
        return ImageFont.load_default()

def draw_table(draw, y_start, headers, data):
    row_height = 40
    col_widths = [c[1] for c in REPORT_COLUMNS]
    
    # Draw headers
    x = 50
    for (header, width), col_w in zip(headers, col_widths):
        draw.rectangle([(x, y_start), (x+col_w, y_start+40)], fill=THEME["header"])
        draw.text((x+10, y_start+5), header, font=load_font(18, True), fill="white")
        x += col_w
    
    # Draw rows
    y = y_start + 40
    for metric, values in data.items():
        x = 50
        for col in headers:
            value = values[col[0].replace(" ", "")]
            color = THEME["positive"] if value >=0 else THEME["negative"]
            draw.text((x+10, y+5), f"{value:.2f}%", font=load_font(16), fill=color)
            x += col[1]
        y += row_height

def generate_infographic(data):
    img = Image.new("RGB", (900, 1200), THEME["background"])
    draw = ImageDraw.Draw(img)
    
    # Title
    title_font = load_font(28, True)
    draw.text((50, 20), f"Daily Market Report - {data['timestamp']}", 
             fill=THEME["text"], font=title_font)
    
    # Main table
    draw_table(draw, 80, REPORT_COLUMNS, data)
    
    # Save with timestamp
    filename = f"Market_Report_{data['timestamp'].replace(':', '-')}.png"
    img.save(filename)
    return filename
