def fetch_market_data() -> Optional[Dict[str, Any]]:
    """Main function to fetch all market data"""
    cg = CoinGeckoAPI()
    
    # Time handling with proper timezone conversion
    utc_now = datetime.now(timezone.utc)
    sast_time = utc_now.astimezone(timezone(timedelta(hours=2)))
    
    # Determine report time based on current UTC hour
    if utc_now.hour == 3:  # Early morning update
        report_time = sast_time.replace(hour=5, minute=0)
    elif utc_now.hour == 15:  # Afternoon update
        report_time = sast_time.replace(hour=17, minute=0)
    else:
        report_time = sast_time

    try:
        # Use the FTSE/JSE All‑Share Index ticker
        jse_tickers = ["^J203.JO"]  # Yahoo Finance ticker for FTSE/JSE All‑Share Index :contentReference[oaicite:0]{index=0}
        jse = None
        jse_ticker_used = None
        
        for ticker in jse_tickers:
            jse = get_latest_price(ticker)
            if jse is not None:
                jse_ticker_used = ticker
                break
        
        if jse is None:
            print("⚠️ Could not fetch JSE All‑Share data from ^J203.JO")
            return None

        # Fetch other market data
        zarusd = get_latest_price("ZAR=X")
        eurzar = get_latest_price("EURZAR=X")
        gbpzar = get_latest_price("GBPZAR=X")
        brent = get_latest_price("BZ=F")
        gold = get_latest_price("GC=F")
        sp500 = get_latest_price("^GSPC")

        try:
            bitcoin_data = cg.get_price(ids="bitcoin", vs_currencies="zar")
            bitcoin = bitcoin_data["bitcoin"]["zar"]
        except Exception as e:
            print(f"⚠️ Bitcoin current price error: {str(e)}")
            bitcoin = None

        # Historical and YTD prices
        jse_1d = fetch_historical(jse_ticker_used, 1)
        jse_ytd = get_ytd_reference_price(jse_ticker_used)

        # ... (rest of your historical/YTD fetches remain unchanged)

        result = {
            "timestamp": report_time.strftime("%Y-%m-%d %H:%M"),
            "JSEALSHARE": {
                "Today": jse,
                "Change": calculate_percentage(jse_1d, jse),
                "Monthly": calculate_percentage(fetch_historical(jse_ticker_used, 30), jse),
                "YTD": calculate_percentage(jse_ytd, jse)
            },
            # ... (the rest of your result dictionary)
        }
        
        return result
        
    except Exception as e:
        print(f"❌ Critical error in fetch_market_data: {str(e)}")
        return None
