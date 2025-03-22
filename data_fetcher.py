from datetime import datetime, timezone, timedelta

def fetch_market_data():
    """Main function to fetch all market data with accurate percentages and aligned timestamps"""
    cg = CoinGeckoAPI()
    now_utc = datetime.now(timezone.utc)

    # Force 05:00 or 17:00 SAST timestamps for scheduled runs
    if now_utc.hour == 3:  # 3AM UTC = 5AM SAST
        report_time = (now_utc.replace(hour=5, minute=0, second=0, microsecond=0)
                      .astimezone(timezone(timedelta(hours=2))))
    elif now_utc.hour == 15:  # 3PM UTC = 5PM SAST
        report_time = (now_utc.replace(hour=17, minute=0, second=0, microsecond=0)
                      .astimezone(timezone(timedelta(hours=2))))
    else:
        report_time = now_utc.astimezone(timezone(timedelta(hours=2)))

    timestamp = report_time.strftime("%Y-%m-%d %H:%M")

    try:
        # Current Prices
        jse = yf.Ticker("^JN0U.JO").history(period="1d")["Close"].iloc[-1]
        usdzar = yf.Ticker("USDZAR=X").history(period="1d")["Close"].iloc[-1]
        eurzar = yf.Ticker("EURZAR=X").history(period="1d")["Close"].iloc[-1]
        gbpzar = yf.Ticker("GBPZAR=X").history(period="1d")["Close"].iloc[-1]
        brent = yf.Ticker("BZ=F").history(period="1d")["Close"].iloc[-1]
        gold = yf.Ticker("GC=F").history(period="1d")["Close"].iloc[-1]
        sp500 = yf.Ticker("^GSPC").history(period="1d")["Close"].iloc[-1]
        bitcoin = cg.get_price(ids="bitcoin", vs_currencies="zar")["bitcoin"]["zar"]

        # Historical Prices (1D, 1M, YTD)
        jse_1d = fetch_historical("^JN0U.JO", 1)
        usdzar_1d = fetch_historical("USDZAR=X", 1)
        eurzar_1d = fetch_historical("EURZAR=X", 1)
        gbpzar_1d = fetch_historical("GBPZAR=X", 1)
        brent_1d = fetch_historical("BZ=F", 1)
        gold_1d = fetch_historical("GC=F", 1)
        sp500_1d = fetch_historical("^GSPC", 1)

        # Bitcoin Historical
        now = datetime.now()
        btc_1d = get_bitcoin_history(cg, 1)
        btc_1m = get_bitcoin_history(cg, 30)
        btc_ytd = get_bitcoin_history(cg, (now - datetime(now.year, 1, 1)).days)

        return {
            "timestamp": timestamp,
            "JSEALSHARE": {
                "Today": jse,
                "Change": calculate_percentage(jse_1d, jse),
                "Monthly": calculate_percentage(fetch_historical("^JN0U.JO", 30), jse),
                "YTD": calculate_percentage(fetch_historical("^JN0U.JO", 365), jse)
            },
            "USDZAR": {
                "Today": usdzar,
                "Change": calculate_percentage(usdzar_1d, usdzar),
                "Monthly": calculate_percentage(fetch_historical("USDZAR=X", 30), usdzar),
                "YTD": calculate_percentage(fetch_historical("USDZAR=X", 365), usdzar)
            },
            "EURZAR": {
                "Today": eurzar,
                "Change": calculate_percentage(eurzar_1d, eurzar),
                "Monthly": calculate_percentage(fetch_historical("EURZAR=X", 30), eurzar),
                "YTD": calculate_percentage(fetch_historical("EURZAR=X", 365), eurzar)
            },
            "GBPZAR": {
                "Today": gbpzar,
                "Change": calculate_percentage(gbpzar_1d, gbpzar),
                "Monthly": calculate_percentage(fetch_historical("GBPZAR=X", 30), gbpzar),
                "YTD": calculate_percentage(fetch_historical("GBPZAR=X", 365), gbpzar)
            },
            "BRENT": {
                "Today": brent,
                "Change": calculate_percentage(brent_1d, brent),
                "Monthly": calculate_percentage(fetch_historical("BZ=F", 30), brent),
                "YTD": calculate_percentage(fetch_historical("BZ=F", 365), brent)
            },
            "GOLD": {
                "Today": gold,
                "Change": calculate_percentage(gold_1d, gold),
                "Monthly": calculate_percentage(fetch_historical("GC=F", 30), gold),
                "YTD": calculate_percentage(fetch_historical("GC=F", 365), gold)
            },
            "SP500": {
                "Today": sp500,
                "Change": calculate_percentage(sp500_1d, sp500),
                "Monthly": calculate_percentage(fetch_historical("^GSPC", 30), sp500),
                "YTD": calculate_percentage(fetch_historical("^GSPC", 365), sp500)
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
