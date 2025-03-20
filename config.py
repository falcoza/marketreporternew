import os
from datetime import datetime

# Email Configuration
EMAIL_SENDER = "ypanchia@gmail.com"
EMAIL_PASSWORD = os.getenv("GITHUBACTIONS_PASSWORD")
EMAIL_RECEIVER = "yeshiel@dailymaverick.co.za"
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587

# Design Constants
THEME = {
    "background": "#FFFFFF",
    "text": "#1D1D1B",
    "header": "#B31B1B",
    "positive": "#008000",
    "negative": "#FF0000"
}

# Font Configuration
FONT_PATHS = {
    "georgia": "/usr/share/fonts/truetype/msttcorefonts/georgia.ttf",
    "georgia_bold": "/usr/share/fonts/truetype/msttcorefonts/georgiab.ttf"
}

# Report Layout
REPORT_COLUMNS = [
    ("Metric", 130),
    ("Today", 90),
    ("1D%", 65),
    ("1M%", 65),
    ("YTD%", 65)
]
