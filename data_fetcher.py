import yfinance as yf
from pycoingecko import CoinGeckoAPI
from datetime import datetime, timezone, timedelta
import pytz

def calculate_percentage(old, new):
    """Calculate percentage change with null safety"""
    if None in (old, new) or old == 0:
        return 0.0
    try:
        return ((new - old) / old) * 100
    except ZeroDivisionError:
        return 0.0

def fetch_historical(ticker, days):
    """Get historical price accounting for non-trading days"""
    try:
        data = yf.Ticker(ticker).history(
            period=f"{days + 5}d",
            interval="1d"
        )
        if not data.empty and len(data) >= days:
            return data['Close'].iloc[-days-1]
        return None
    except Exception as e:
        print(f"⚠️ Historical data error for {ticker}: {str(e)}")
        return None

def get_ytd_reference_price(ticker):
    """Fetch the first trading day's closing price of the current year with proper timezone handling"""
    try:
        tkr = yf.Ticker(ticker)
        current_year = datetime.now().year
        
        # Get exchange timezone
        tz_name = tkr.info.get('exchangeTimezoneName', 'UTC')
        tz = pytz.timezone(tz_name)
        
        # Create start and end dates in exchange's timezone
        start_date = tz.localize(datetime(current_year, 1, 1))
        end_date = start_date + timedelta(days=30)
        buffer_start = start_date - timedelta(days=14)
        
        # Convert to UTC for yfinance query
        data = tkr.history(
            start=buffer_start.astimezone(pytz.utc),
            end=end_date.astimezone(pytz.utc),
            interval="1d"
        )
        
        if not data.empty:
            # Convert index to exchange timezone
            data.index = data.index.tz_convert(tz)
            # Filter for dates >= Jan 1 in exchange timezone
            ytd_data = data[data.index >= start_date]
            if not ytd_data.empty:
                return ytd_data['Close'].iloc[0]
        return None
    except Exception as e:
        print(f"⚠️ YTD reference price error for {ticker}: {str(e)}")
        return None

def get_bitcoin_ytd_price(cg):
    """Get Bitcoin price on Jan 1 of current year"""
    try:
        start_date = datetime(datetime.now().year, 1, 1, tzinfo=timezone.utc)
        history = cg.get_coin_market_chart_range_by_id(
            "bitcoin",
            "zar",
            int(start_date.timestamp()),
            int(datetime.now(timezone.utc).timestamp())
        )
        # Find first price on or after Jan 1
        for price in history.get('prices', []):
            if datetime.fromtimestamp(price[0]/1000, tz=timezone.utc) >= start_date:
                return price[1]
        return None
    except Exception as e:
        print(f"⚠️ Bitcoin YTD error: {str(e)}")
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
        def get_latest_price(ticker):
            try:
                data = yf.Ticker(ticker).history(period="2d", interval="1d")
                return data['Close'].iloc[-1] if not data.empty else None
            except Exception as e:
                print(f"⚠️ Price fetch error for {ticker}: {str(e)}")
                return None

        # Current Prices
        jse = get_latest_price("^JN0U.JO")
        zarusd = get_latest_price("ZAR=X")
        eurzar = get_latest_price("EURZAR=X")
        gbpzar = get_latest_price("GBPZAR=X")
        brent = get_latest_price("BZ=F")
        gold = get_latest_price("GC=F")
        sp500 = get_latest_price("^GSPC")
        bitcoin = cg.get_price(ids="bitcoin", vs_currencies="zar")["bitcoin"]["zar"]

        # Historical Prices
        jse_1d = fetch_historical("^JN0U.JO", 1)
        zarusd_1d = fetch_historical("ZAR=X", 1)
        eurzar_1d = fetch_historical("EURZAR=X", 1)
        gbpzar_1d = fetch_historical("GBPZAR=X", 1)
        brent_1d = fetch_historical("BZ=F", 1)
        gold_1d = fetch_historical("GC=F", 1)
        sp500_1d = fetch_historical("^GSPC", 1)

        # YTD Prices
        jse_ytd = get_ytd_reference_price("^JN0U.JO")
        zarusd_ytd = get_ytd_reference_price("ZAR=X")
        eurzar_ytd = get_ytd_reference_price("EURZAR=X")
        gbpzar_ytd = get_ytd_reference_price("GBPZAR=X")
        brent_ytd = get_ytd_reference_price("BZ=F")
        gold_ytd = get_ytd_reference_price("GC=F")
        sp500_ytd = get_ytd_reference_price("^GSPC")
        btc_ytd = get_bitcoin_ytd_price(cg)

        # Use the ZAR value directly for USD/ZAR
        usdzar = zarusd if zarusd else None
        usdzar_1d = zarusd_1d if zarusd_1d else None
        usdzar_ytd = zarusd_ytd if zarusd_ytd else None

        return {
            "timestamp": report_time.strftime("%Y-%m-%d %H:%M"),
            "JSEALSHARE": {
                "Today": jse,
                "Change": calculate_percentage(jse_1d, jse),
                "Monthly": calculate_percentage(fetch_historical("^JN0U.JO", 30), jse),
                "YTD": calculate_percentage(jse_ytd, jse)
            },
            "USDZAR": {
                "Today": usdzar,
                "Change": calculate_percentage(usdzar_1d, usdzar),
                "Monthly": calculate_percentage(fetch_historical("ZAR=X", 30), usdzar),
                "YTD": calculate_percentage(usdzar_ytd, usdzar)
            },
            "EURZAR": {
                "Today": eurzar,
                "Change": calculate_percentage(eurzar_1d, eurzar),
                "Monthly": calculate_percentage(fetch_historical("EURZAR=X", 30), eurzar),
                "YTD": calculate_percentage(eurzar_ytd, eurzar)
            },
            "GBPZAR": {
                "Today": gbpzar,
                "Change": calculate_percentage(gbpzar_1d, gbpzar),
                "Monthly": calculate_percentage(fetch_historical("GBPZAR=X", 30), gbpzar),
                "YTD": calculate_percentage(gbpzar_ytd, gbpzar)
            },
            "BRENT": {
                "Today": brent,
                "Change": calculate_percentage(brent_1d, brent),
                "Monthly": calculate_percentage(fetch_historical("BZ=F", 30), brent),
                "YTD": calculate_percentage(brent_ytd, brent)
            },
            "GOLD": {
                "Today": gold,
                "Change": calculate_percentage(gold_1d, gold),
                "Monthly": calculate_percentage(fetch_historical("GC=F", 30), gold),
                "YTD": calculate_percentage(gold_ytd, gold)
            },
            "SP500": {
                "Today": sp500,
                "Change": calculate_percentage(sp500_1d, sp500),
                "Monthly": calculate_percentage(fetch_historical("^GSPC", 30), sp500),
                "YTD": calculate_percentage(sp500_ytd, sp500)
            },
            "BITCOINZAR": {
                "Today": bitcoin,
                "Change": calculate_percentage(fetch_historical("BTC-ZAR", 1), bitcoin),
                "Monthly": calculate_percentage(fetch_historical("BTC-ZAR", 30), bitcoin),
                "YTD": calculate_percentage(btc_ytd, bitcoin)
            }
        }
        
    except Exception as e:
        print(f"❌ Critical error: {str(e)}")
        return None
