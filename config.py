import os

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

REPORT_COLUMNS = [
    ("Metric", 120),
    ("Today", 80),
    ("1D%", 60),
    ("1M%", 60),
    ("YTD%", 60)
]
