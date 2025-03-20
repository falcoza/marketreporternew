import os
from datetime import datetime

EMAIL_SENDER = "ypanchia@gmail.com"
EMAIL_PASSWORD = os.getenv("GITHUBACTIONS_PASSWORD")
EMAIL_RECEIVERS = [
    "yeshiel@dailymaverick.co.za",
    "lisakhanya@dailymaverick.co.za", 
    "neesa@dailymaverick.co.za"
]
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587

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

REPORT_COLUMNS = [
    ("Metric", 160),
    ("Today", 120),
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
