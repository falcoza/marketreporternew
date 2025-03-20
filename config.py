import os

# Email Configuration
EMAIL_SENDER = "your-email@gmail.com"  # Must match EXACT Gmail account
EMAIL_PASSWORD = os.getenv("GITHUBACTIONS_PASSWORD")
EMAIL_RECEIVER = "yeshiel@dailymaverick.co.za"
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587

# Design Constants
THEME = {
    "background": "#FFFFFF",
    "text": "#1D1D1B",
    "header": "#B31B1B",
    "border": "#D3D3D3",
    "positive": "#008000",
    "negative": "#FF0000"
}

FONT_PATHS = {
    "georgia": "/usr/share/fonts/truetype/msttcorefonts/georgia.ttf",
    "georgia_bold": "/usr/share/fonts/truetype/msttcorefonts/georgiab.ttf"
}

REPORT_COLUMNS = [
    ("Today", 150),
    ("% Change", 120),
    ("vs Last Month", 150),
    ("YTD", 100)
]
