import os
from datetime import datetime

# Email Configuration
EMAIL_SENDER = "ypanchia@gmail.com"  # Must match EXACT Gmail address
EMAIL_PASSWORD = os.getenv("GITHUBACTIONS_PASSWORD")  # From GitHub Secrets
EMAIL_RECEIVER = "yeshiel@dailymaverick.co.za"
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587

# Design Constants
THEME = {
    "background": "#FFFFFF",
    "text": "#1D1D1B",
    "header": "#B31B1B",
    "border": "#D3D3D3",
    "positive": "#008000",  # Green
    "negative": "#FF0000"   # Red
}

# Font Configuration (GitHub Actions Paths)
FONT_PATHS = {
    "georgia": "/usr/share/fonts/truetype/msttcorefonts/georgia.ttf",
    "georgia_bold": "/usr/share/fonts/truetype/msttcorefonts/georgiab.ttf"
}

# Report Layout (Pixel Widths)
REPORT_COLUMNS = [
    ("Metric", 180),
    ("Today", 140),
    ("Daily %", 110),
    ("1M %", 110),
    ("YTD %", 110)
]

# Execution Timestamp
REPORT_TIMESTAMP = datetime.now().strftime("%Y-%m-%d_%H-%M")
