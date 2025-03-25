import yfinance as yf
from pycoingecko import CoinGeckoAPI
from datetime import datetime, timezone, timedelta

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
        # Get extra data to account for weekends/holidays
        data = yf.Ticker(ticker).history(
            period=f"{days + 5}d",  # Buffer for non-trading days
            interval="1d"
        )
        
        if not data.empty and len(data) >= days:
            return data['Close'].iloc[-days-1]
        return None
    except Exception as e:
        print(f"⚠️ Historical data error for {ticker}: {str(e)}")
        return None

def get_ytd_reference_price(ticker):
    """Fetch the first trading day's closing price of the current year.
    
    To ensure correct behavior all year, the query is restricted to a fixed window
    (the first 30 days of January) so that it always retrieves the early-year data.
    """
    try:
        current_year = datetime.now().year
        start_date = datetime(current_year, 1, 1)
        end_date = start_date + timedelta(days=30)  # Fixed window: first 30 days of January
        
        data = yf.Ticker(ticker).history(
            start=start_date - timedelta(days=14),  # Buffer before Jan 1
            end=end_date,
            interval="1d"
        )
        
        if not data.empty:
            # Filter for dates on or after Jan 1 to get actual trading days.
            ytd_data = data.loc[data.index >= start_date]
            if not ytd_data.empty:
                return ytd_data['Close'].iloc[0]
        return None
    except Exception as e:
        print(f"⚠️ YTD reference price error for {ticker}: {str(e)}")
        return None

def get_bitcoin_history(cg, days):
    """Get precise Bitcoin historical price using exact timestamps"""
    try:
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=days)
        history = cg.get_coin_market_chart_range_by_id(
            "bitcoin",
            "zar",
            int(start_date.timestamp()),
            int(end_date.timestamp())
        )
        return history['prices'][0][1] if history['prices'] else None
    except Exception as e:
        print(f"⚠️ Bitcoin history error: {str(e)}")
        return None

def fetch_market_data():
    """Main function to fetch all market data with SAST timestamps"""
    cg = CoinGeckoAPI()
    
    # Get current time in UTC and convert to SAST (UTC+2)
    utc_now = datetime.now(timezone.utc)
    sast_offset = timedelta(hours=2)
    sast_time = utc_now.astimezone(timezone(sast_offset))
    
    # For scheduled runs, force 05:00 or 17:00 SAST timestamps
    if utc_now.hour == 3:    # 3AM UTC = 5AM SAST
        report_time = sast_time.replace(hour=5, minute=0, second=0, microsecond=0)
    elif utc_now.hour == 15: # 3PM UTC = 5PM SAST
        report_time = sast_time.replace(hour=17, minute=0, second=0, microsecond=0)
    else:
        report_time = sast_time
    
    timestamp = report_time.strftime("%Y-%m-%d %H:%M")

    try:
        # ===== IMPROVED DATA FETCHING =====
        def get_latest_price(ticker):
            """Get the most recent closing price with forced refresh"""
            try:
                data = yf.Ticker(ticker).history(
                    period="2d",  # Ensures we get at least 1 trading day
                    interval="1d",
                    prepost=False  # Only official market hours
                )
                if not data.empty:
                    return data['Close'].dropna().iloc[-1]  # Last valid close
                return None
            except Exception as e:
                print(f"⚠️ Error fetching {ticker}: {str(e)}")
                return None

        # Current Prices (using improved fetcher)
        jse = get_latest_price("^JN0U.JO")
        zarusd = get_latest_price("ZAR=X")  # This is ZAR/USD (how many USD per 1 ZAR)
        eurzar = get_latest_price("EURZAR=X")  # This is EUR/ZAR (how many ZAR per 1 EUR)
        gbpzar = get_latest_price("GBPZAR=X")  # This is GBP/ZAR (how many ZAR per 1 GBP)
        brent = get_latest_price("BZ=F")
        gold = get_latest_price("GC=F")
        sp500 = get_latest_price("^GSPC")
        bitcoin = cg.get_price(ids="bitcoin", vs_currencies="zar")["bitcoin"]["zar"]

        # Debug output to verify fresh data
        print(f"DEBUG - JSE: {jse} (Updated: {datetime.now().strftime('%H:%M')})")
        print(f"DEBUG - S&P 500: {sp500} (Updated: {datetime.now().strftime('%H:%M')})")

        # Historical Prices (1D, 1M)
        jse_1d = fetch_historical("^JN0U.JO", 1)
        zarusd_1d = fetch_historical("ZAR=X", 1)
        eurzar_1d = fetch_historical("EURZAR=X", 1)
        gbpzar_1d = fetch_historical("GBPZAR=X", 1)
        brent_1d = fetch_historical("BZ=F", 1)
        gold_1d = fetch_historical("GC=F", 1)
        sp500_1d = fetch_historical("^GSPC", 1)

        # YTD Reference Prices (First trading day of the year)
        jse_ytd = get_ytd_reference_price("^JN0U.JO")
        zarusd_ytd = get_ytd_reference_price("ZAR=X")
        eurzar_ytd = get_ytd_reference_price("EURZAR=X")
        gbpzar_ytd = get_ytd_reference_price("GBPZAR=X")
        brent_ytd = get_ytd_reference_price("BZ=F")
        gold_ytd = get_ytd_reference_price("GC=F")
        sp500_ytd = get_ytd_reference_price("^GSPC")

        # Bitcoin Historical
        btc_1d = get_bitcoin_history(cg, 1)
        btc_1m = get_bitcoin_history(cg, 30)
        btc_ytd = get_bitcoin_history(cg, (datetime.now(timezone.utc) - datetime(datetime.now().year, 1, 1, tzinfo=timezone.utc)).days)

        # Calculate USD/ZAR from ZAR/USD
        usdzar = 1/zarusd if zarusd else None
        usdzar_1d = 1/zarusd_1d if zarusd_1d else None
        usdzar_ytd = 1/zarusd_ytd if zarusd_ytd else None

        return {
            "timestamp": timestamp,
            "JSEALSHARE": {
                "Today": jse,
                "Change": calculate_percentage(jse_1d, jse),
                "Monthly": calculate_percentage(fetch_historical("^JN0U.JO", 30), jse),
                "YTD": calculate_percentage(jse_ytd, jse)
            },
            "USDZAR": {
                "Today": usdzar,
                "Change": calculate_percentage(usdzar_1d, usdzar),
                "Monthly": calculate_percentage(1/fetch_historical("ZAR=X", 30) if fetch_historical("ZAR=X", 30) else None, usdzar),
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
                "Change": calculate_percentage(btc_1d, bitcoin),
                "Monthly": calculate_percentage(btc_1m, bitcoin),
                "YTD": calculate_percentage(btc_ytd, bitcoin)
            }
        }
        
    except Exception as e:
        print(f"❌ Critical data fetch error: {str(e)}")
        return None
