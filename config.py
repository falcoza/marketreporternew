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

FONT_PATHS = {
    "georgia": "/usr/share/fonts/truetype/msttcorefonts/georgia.ttf",
    "georgia_bold": "/usr/share/fonts/truetype/msttcorefonts/georgiab.ttf"
}

# 480px width layout: 140+100+70+70+70 = 450px + 30px padding
REPORT_COLUMNS = [
    ("Metric", 140),
    ("Today", 100),
    ("1D%", 70),
    ("1M%", 70),
    ("YTD%", 70)
]

REQUIRED_KEYS = [
    "JSEALSHARE",
    "USDZAR",
    "EURZAR",
    "GBPZAR",
    "BRENT",
    "GOLD",
    "SP500",
    "BITCOINZAR"
]

def validate_data(data):
    missing = [key for key in REQUIRED_KEYS if key not in data]
    if missing:
        raise ValueError(f"Missing data keys: {', '.join(missing)}")
    return True
