import os

# Email Configuration
EMAIL_SENDER = "your-email@gmail.com"
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")  # From GitHub Secrets
EMAIL_RECEIVER = "yeshiel@dailymaverick.co.za"
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587

# Design Constants
THEME = {
    "background": "#FFFFFF",
    "text": "#1D1D1B",
    "header": "#B31B1B",
    "border": "#D3D3D3"
}

# Data Sources
ASSETS = {
    "jse": "^JN0U.JO",
    "forex": ["USDZAR=X", "EURZAR=X", "GBPZAR=X"],
    "commodities": ["BZ=F", "GC=F"],
    "sp500": "^GSPC",
    "crypto": "bitcoin"
}
