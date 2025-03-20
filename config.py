import os
from datetime import datetime

# Email Configuration
EMAIL_SENDER = "your-email@gmail.com"
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
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

# Font Configuration
FONT_PATHS = {
    "georgia": "/usr/share/fonts/truetype/msttcorefonts/georgia.ttf",
    "georgia_bold": "/usr/share/fonts/truetype/msttcorefonts/georgiab.ttf"
}

# Report Formatting
REPORT_COLUMNS = [
    ("Today", 150),
    ("% Change", 120),
    ("vs Last Month", 150),
    ("YTD", 100)
]
