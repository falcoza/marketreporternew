from googlefinance import getQuotes
import json
from pycoingecko import CoinGeckoAPI
from datetime import datetime, timezone, timedelta
import pytz
import requests  # For MarketStack fallback

# Configuration
MARKETSTACK_API_KEY = 'YOUR_API_KEY'  # Get from marketstack.com

def calculate_percentage(old, new):
    """Calculate percentage change with null safety"""
    if None in (old, new) or old == 0:
        return 0.0
    try:
        return ((new - old) / old) * 100
    except ZeroDivisionError:
        return 0.0

def get_googlefinance_price(ticker):
    """Get current price using Google Finance"""
    try:
        quotes = getQuotes(ticker)
        if quotes and 'LastTradePrice' in quotes[0]:
            return float(quotes[0]['LastTradePrice'])
        return None
    except:
        return None

def get_marketstack_price(ticker):
    """Fallback using MarketStack API"""
    try:
        response = requests.get(
            f"http://api.marketstack.com/v1/tickers/{ticker}/eod/latest",
            params={'access_key': MARKETSTACK_API_KEY}
        )
        data = response.json()
        return data.get('close') if response.status_code == 200 else None
    except:
        return None

def get_current_price(ticker):
    """Try Google Finance first, then fallback to MarketStack"""
    price = get_googlefinance_price(ticker)
    if price is None:
        price = get_marketstack_price(ticker)
    return price

def get_jse_allshare():
    """Special handling for JSE All Share"""
    # Try various ticker representations
    for ticker in ["JSE:J203", "JSE:JALSH", "JSE:JSE"]:
        price = get_current_price(ticker)
        if price is not None:
            return price
    return None

def get_bitcoin_data(cg):
    """Get Bitcoin data from CoinGecko"""
    try:
        current_price = cg.get_price(ids="bitcoin", vs_currencies="zar")["bitcoin"]["zar"]
        
        # Historical data (simplified)
        now = datetime.now(timezone.utc)
        one_day_ago = now - timedelta(days=1)
        thirty_days_ago = now - timedelta(days=30)
        ytd_start = datetime(now.year, 1, 1, tzinfo=timezone.utc)
        
        history = cg.get_coin_market_chart_range_by_id(
            "bitcoin", "zar",
            int(ytd_start.timestamp()),
            int(now.timestamp())
        )
        
        def find_closest_price(prices, target_date):
            for price in prices:
                if datetime.fromtimestamp(price[0]/1000, tz=timezone.utc) >= target_date:
                    return price[1]
            return None
        
        return {
            "current": current_price,
            "one_day": find_closest_price(history['prices'], one_day_ago),
            "thirty_day": find_closest_price(history['prices'], thirty_days_ago),
            "ytd": find_closest_price(history['prices'], ytd_start)
        }
    except Exception as e:
        print(f"⚠️ Bitcoin data error: {str(e)}")
        return None

def fetch_market_data():
    cg = CoinGeckoAPI()
    
    # Time handling
    utc_now = datetime.now(timezone.utc)
    sast_time = utc_now.astimezone(timezone(timedelta(hours=2)))
    report_time = sast_time.strftime("%Y-%m-%d %H:%M")

    try:
        # Current Prices
        jse = get_jse_allshare()
        usdzar = get_current_price("USDZAR")
        eurzar = get_current_price("EURZAR")
        gbpzar = get_current_price("GBPZAR")
        brent = get_current_price("BRENT")
        gold = get_current_price("GC=F")
        sp500 = get_current_price("SPX")
        
        # Bitcoin data
        bitcoin_data = get_bitcoin_data(cg)
        bitcoin = bitcoin_data["current"] if bitcoin_data else None

        # Historical prices (simplified - in production you'd want proper historical data)
        # Note: Google Finance doesn't provide easy historical data access
        # This is a simplified approach - consider using MarketStack for proper historical data
        jse_1d = jse * 0.99  # Placeholder - replace with actual historical data
        usdzar_1d = usdzar * 0.99 if usdzar else None
        # ... similar for others

        return {
            "timestamp": report_time,
            "JSEALSHARE": {
                "Today": jse,
                "Change": calculate_percentage(jse_1d, jse),
                "Monthly": 0.0,  # Placeholder
                "YTD": 0.0       # Placeholder
            },
            "USDZAR": {
                "Today": usdzar,
                "Change": calculate_percentage(usdzar_1d, usdzar),
                "Monthly": 0.0,
                "YTD": 0.0
            },
            # ... other market data similarly
            "BITCOINZAR": {
                "Today": bitcoin,
                "Change": calculate_percentage(bitcoin_data.get("one_day"), bitcoin) if bitcoin_data else 0.0,
                "Monthly": calculate_percentage(bitcoin_data.get("thirty_day"), bitcoin) if bitcoin_data else 0.0,
                "YTD": calculate_percentage(bitcoin_data.get("ytd"), bitcoin) if bitcoin_data else 0.0
            }
        }
        
    except Exception as e:
        print(f"❌ Critical error: {str(e)}")
        return None
