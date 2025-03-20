from PIL import Image, ImageDraw, ImageFont
import pandas as pd
from config import THEME

def generate_infographic(data):
    """Create branded infographic with proper error handling"""
    try:
        # Setup image
        img = Image.new("RGB", (900, 600), THEME["background"])
        draw = ImageDraw.Draw(img)
        
        # Font handling with fallbacks
        try:
            font_path = "/usr/share/fonts/truetype/msttcorefonts/georgia.ttf"  # GitHub Actions path
            title_font = ImageFont.truetype(font_path, 36)
            header_font = ImageFont.truetype(font_path, 24)
            text_font = ImageFont.truetype(font_path, 22)
        except:
            title_font = ImageFont.load_default()
            header_font = ImageFont.load_default()
            text_font = ImageFont.load_default()

        # Add title
        title = f"Daily Market Report - {data['Date']}"
        draw.text((50, 20), title, fill=THEME["text"], font=title_font)

        # Create table
        y_position = 80
        headers = ["Metric", "Value"]
        
        # Draw header
        draw.rectangle([(50, y_position), (850, y_position+40)], fill=THEME["header"])
        for i, header in enumerate(headers):
            x = 100 if i == 0 else 600
            draw.text((x, y_position+5), header, font=header_font, fill="white")

        # Draw rows
        y_position += 50
        for idx, (key, value) in enumerate(data.items()):
            if key == "Date": continue
            
            bg_color = "#F5F5F5" if idx%2 == 0 else THEME["background"]
            draw.rectangle([(50, y_position), (850, y_position+40)], fill=bg_color)
            
            draw.text((100, y_position+5), key, font=text_font, fill=THEME["text"])
            draw.text((600, y_position+5), f"{value:.2f}", font=text_font, fill=THEME["text"])
            y_position += 40

        img.save("market_report.png")
        return True
    
    except Exception as e:
        print(f"Infographic generation failed: {e}")
        return False
