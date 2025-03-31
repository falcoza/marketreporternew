import requests
from pycoingecko import CoinGeckoAPI
from datetime import datetime, timezone, timedelta
import pytz

MARKETSTACK_API_KEY = "0b08010fbec3effa90c076a59cf7119d"  # Replace with your actual Marketstack API key

def calculate_percentage(old, new):
    """Calculate percentage change with null safety"""
    if None in (old, new) or old == 0:
        return 0.0
    try:
        return ((new - old) / old) * 100
    except ZeroDivisionError:
        return 0.0

def fetch_marketstack_data(symbol, date=None):
    """Fetch data from Marketstack API"""
    try:
        base_url = "http://api.marketstack.com/v1/"
        endpoint = "eod" if date else "intraday/latest"
        
        params = {
            'access_key': MARKETSTACK_API_KEY,
            'symbols': symbol
        }
        
        if date:
            params['date_from'] = date.strftime('%Y-%m-%d')
            params['date_to'] = date.strftime('%Y-%m-%d')
        
        response = requests.get(base_url + endpoint, params=params)
        response.raise_for_status()
        data = response.json()
        
        if data.get('data'):
            if isinstance(data['data'], list):
                return data['data'][0]['close'] if data['data'] else None
            return data['data']['close']
        return None
    except Exception as e:
        print(f"⚠️ Marketstack error for {symbol}: {str(e)}")
        return None

def fetch_historical(ticker, days):
    """Get historical price accounting for non-trading days"""
    try:
        target_date = datetime.now(timezone.utc) - timedelta(days=days)
        return fetch_marketstack_data(ticker, target_date)
    except Exception as e:
        print(f"⚠️ Historical data error for {ticker}: {str(e)}")
        return None

def get_ytd_reference_price(ticker):
    """Fetch the first trading day's closing price of the current year"""
    try:
        current_year = datetime.now().year
        start_date = datetime(current_year, 1, 1)
        
        # Try to get data for Jan 1, if not available, try subsequent days
        for day_offset in range(0, 30):
            current_date = start_date + timedelta(days=day_offset)
            price = fetch_marketstack_data(ticker, current_date)
            if price is not None:
                return price
        return None
    except Exception as e:
        print(f"⚠️ YTD reference price error for {ticker}: {str(e)}")
        return None

def get_bitcoin_data(cg):
    """Get Bitcoin price data including current and historical prices in ZAR"""
    try:
        # Current price
        current_price = cg.get_price(ids="bitcoin", vs_currencies="zar")["bitcoin"]["zar"]
        
        # YTD price
        start_date = datetime(datetime.now().year, 1, 1, tzinfo=timezone.utc)
        history = cg.get_coin_market_chart_range_by_id(
            "bitcoin",
            "zar",
            int(start_date.timestamp()),
            int(datetime.now(timezone.utc).timestamp())
        )
        # Find first price on or after Jan 1
        ytd_price = None
        for price in history.get('prices', []):
            if datetime.fromtimestamp(price[0]/1000, tz=timezone.utc) >= start_date:
                ytd_price = price[1]
                break
        
        # 1-day price
        one_day_ago = datetime.now(timezone.utc) - timedelta(days=1)
        one_day_price = None
        for price in history.get('prices', []):
            if datetime.fromtimestamp(price[0]/1000, tz=timezone.utc) >= one_day_ago:
                one_day_price = price[1]
                break
        
        # 30-day price
        thirty_days_ago = datetime.now(timezone.utc) - timedelta(days=30)
        thirty_day_price = None
        for price in history.get('prices', []):
            if datetime.fromtimestamp(price[0]/1000, tz=timezone.utc) >= thirty_days_ago:
                thirty_day_price = price[1]
                break
        
        return {
            "current": current_price,
            "ytd": ytd_price,
            "one_day": one_day_price,
            "thirty_day": thirty_day_price
        }
    except Exception as e:
        print(f"⚠️ Bitcoin data error: {str(e)}")
        return None

def fetch_market_data():
    cg = CoinGeckoAPI()
    
    # SAST time handling
    utc_now = datetime.now(timezone.utc)
    sast_time = utc_now.astimezone(timezone(timedelta(hours=2)))
    
    if utc_now.hour == 3:
        report_time = sast_time.replace(hour=5, minute=0)
    elif utc_now.hour == 15:
        report_time = sast_time.replace(hour=17, minute=0)
    else:
        report_time = sast_time

    try:
        # Current Prices
        jse = fetch_marketstack_data("JALSH.JO")  # JSE All Share Index
        zarusd = fetch_marketstack_data("USDZAR")  # USD/ZAR
        eurzar = fetch_marketstack_data("EURZAR")  # EUR/ZAR
        gbpzar = fetch_marketstack_data("GBPZAR")  # GBP/ZAR
        brent = fetch_marketstack_data("BRENT")    # Brent Crude
        gold = fetch_marketstack_data("XAU")       # Gold
        sp500 = fetch_marketstack_data("SPX")      # S&P 500
        
        # Bitcoin data
        bitcoin_data = get_bitcoin_data(cg)
        bitcoin = bitcoin_data["current"] if bitcoin_data else None

        # Historical Prices (1 day ago)
        jse_1d = fetch_historical("JALSH.JO", 1)
        zarusd_1d = fetch_historical("USDZAR", 1)
        eurzar_1d = fetch_historical("EURZAR", 1)
        gbpzar_1d = fetch_historical("GBPZAR", 1)
        brent_1d = fetch_historical("BRENT", 1)
        gold_1d = fetch_historical("XAU", 1)
        sp500_1d = fetch_historical("SPX", 1)

        # YTD Prices
        jse_ytd = get_ytd_reference_price("JALSH.JO")
        zarusd_ytd = get_ytd_reference_price("USDZAR")
        eurzar_ytd = get_ytd_reference_price("EURZAR")
        gbpzar_ytd = get_ytd_reference_price("GBPZAR")
        brent_ytd = get_ytd_reference_price("BRENT")
        gold_ytd = get_ytd_reference_price("XAU")
        sp500_ytd = get_ytd_reference_price("SPX")
        
        # Bitcoin historical data
        btc_1d = bitcoin_data["one_day"] if bitcoin_data else None
        btc_30d = bitcoin_data["thirty_day"] if bitcoin_data else None
        btc_ytd = bitcoin_data["ytd"] if bitcoin_data else None

        # Use the ZAR value directly for USD/ZAR
        usdzar = zarusd if zarusd else None
        usdzar_1d = zarusd_1d if zarusd_1d else None
        usdzar_ytd = zarusd_ytd if zarusd_ytd else None

        return {
            "timestamp": report_time.strftime("%Y-%m-%d %H:%M"),
            "JSEALSHARE": {
                "Today": jse,
                "Change": calculate_percentage(jse_1d, jse),
                "Monthly": calculate_percentage(fetch_historical("JALSH.JO", 30), jse),
                "YTD": calculate_percentage(jse_ytd, jse)
            },
            "USDZAR": {
                "Today": usdzar,
                "Change": calculate_percentage(usdzar_1d, usdzar),
                "Monthly": calculate_percentage(fetch_historical("USDZAR", 30), usdzar),
                "YTD": calculate_percentage(usdzar_ytd, usdzar)
            },
            "EURZAR": {
                "Today": eurzar,
                "Change": calculate_percentage(eurzar_1d, eurzar),
                "Monthly": calculate_percentage(fetch_historical("EURZAR", 30), eurzar),
                "YTD": calculate_percentage(eurzar_ytd, eurzar)
            },
            "GBPZAR": {
                "Today": gbpzar,
                "Change": calculate_percentage(gbpzar_1d, gbpzar),
                "Monthly": calculate_percentage(fetch_historical("GBPZAR", 30), gbpzar),
                "YTD": calculate_percentage(gbpzar_ytd, gbpzar)
            },
            "BRENT": {
                "Today": brent,
                "Change": calculate_percentage(brent_1d, brent),
                "Monthly": calculate_percentage(fetch_historical("BRENT", 30), brent),
                "YTD": calculate_percentage(brent_ytd, brent)
            },
            "GOLD": {
                "Today": gold,
                "Change": calculate_percentage(gold_1d, gold),
                "Monthly": calculate_percentage(fetch_historical("XAU", 30), gold),
                "YTD": calculate_percentage(gold_ytd, gold)
            },
            "SP500": {
                "Today": sp500,
                "Change": calculate_percentage(sp500_1d, sp500),
                "Monthly": calculate_percentage(fetch_historical("SPX", 30), sp500),
                "YTD": calculate_percentage(sp500_ytd, sp500)
            },
            "BITCOINZAR": {
                "Today": bitcoin,
                "Change": calculate_percentage(btc_1d, bitcoin),
                "Monthly": calculate_percentage(btc_30d, bitcoin),
                "YTD": calculate_percentage(btc_ytd, bitcoin)
            }
        }
        
    except Exception as e:
        print(f"❌ Critical error: {str(e)}")
        return None
