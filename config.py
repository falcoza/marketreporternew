import os
from datetime import datetime

# Email Configuration
EMAIL_SENDER = "ypanchia@gmail.com"  # Replace with your exact Gmail
EMAIL_PASSWORD = os.getenv("GITHUBACTIONS_PASSWORD")  # From GitHub Secrets
EMAIL_RECEIVER = "yeshiel@dailymaverick.co.za"
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587

# Design Constants
THEME = {
    "background": "#FFFFFF",    # Pure white background
    "text": "#1D1D1B",          # Dark gray text (Daily Maverick brand)
    "header": "#B31B1B",        # Dark red header (DM accent color)
    "positive": "#008000",      # Green for increases
    "negative": "#FF0000"       # Red for decreases
}

# Font Configuration (Ubuntu Paths)
FONT_PATHS = {
    "georgia": "/usr/share/fonts/truetype/msttcorefonts/georgia.ttf",
    "georgia_bold": "/usr/share/fonts/truetype/msttcorefonts/georgiab.ttf"
}

# Column Widths (Total: 130+90+65+65+65 = 415px + 35px padding = 450px)
REPORT_COLUMNS = [
    ("Metric", 130),   # Increased for better text fit
    ("Today", 90),     # Wider for numeric values
    ("1D%", 65),       # Perfect for +/-##.#%
    ("1M%", 65), 
    ("YTD%", 65)
]

# Timestamp Format (Matches filename)
REPORT_TIMESTAMP = datetime.now().strftime("%Y-%m-%d %H:%M")
