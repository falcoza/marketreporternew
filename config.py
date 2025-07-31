import os
from datetime import datetime

# Email Configuration
EMAIL_SENDER = "ypanchia@gmail.com"
EMAIL_PASSWORD = os.getenv("GITHUBACTIONS_PASSWORD")
EMAIL_RECEIVERS = [
    "yeshiel@dailymaverick.co.za"
]
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587

# Design Constants
THEME = {
    "background": "#FFFFFF",
    "text": "#1D1D1B",
    "header": "#005782",
    "positive": "#008000",
    "negative": "#FF0000"
}

# Font Configuration
FONT_PATHS = {
    "georgia": "/usr/share/fonts/truetype/msttcorefonts/georgia.ttf",
    "georgia_bold": "/usr/share/fonts/truetype/msttcorefonts/georgiab.ttf"
}

# Report Layout (520px width)
REPORT_COLUMNS = [
    ("Metric", 160),
    ("Today", 120),
    ("1D%", 70),
    ("1M%", 70),
    ("YTD%", 70)
]

# Data Validation
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
