def fetch_market_data() -> Optional[Dict[str, Any]]:
    """Main function to fetch all market data with fallbacks and logging."""
    cg = CoinGeckoAPI()
    utc_now = datetime.now(timezone.utc)
    sast_time = utc_now.astimezone(timezone(timedelta(hours=2)))
    report_time = sast_time.replace(minute=0)

    try:
        # Fetch JSE (fallback cycle)
        jse_tickers = ["^J203.JO", "J203.JO"]
        jse, jse_ticker = None, None
        for tk in jse_tickers:
            price = get_latest_price(tk)
            if price:
                jse, jse_ticker = price, tk
                break
        if not jse:
            print("⚠️ Could not fetch JSE All Share Index")
        
        # Currency and commodity
        usd_zar   = get_latest_price("ZAR=X")
        eur_zar   = get_latest_price("EURZAR=X")
        gbp_zar   = get_latest_price("GBPZAR=X")
        brent     = get_latest_price("BZ=F")
        gold      = get_latest_price("GC=F")
        sp500     = get_latest_price("^GSPC")

        try:
            bitcoin = cg.get_price(ids="bitcoin", vs_currencies="zar")["bitcoin"]["zar"]
        except Exception as e:
            print(f"⚠️ Bitcoin fetch error: {e}")
            bitcoin = None

        # Historical data (1D)
        jse_1d    = fetch_historical(jse_ticker, 1) if jse_ticker else None
        usd_1d    = fetch_historical("ZAR=X", 1)
        eur_1d    = fetch_historical("EURZAR=X", 1)
        gbp_1d    = fetch_historical("GBPZAR=X", 1)
        brent_1d  = fetch_historical("BZ=F", 1)
        gold_1d   = fetch_historical("GC=F", 1)
        sp_1d     = fetch_historical("^GSPC", 1)
        btc_1d    = fetch_bitcoin_historical(cg, 1)

        # Historical data (1M)
        jse_1m    = fetch_historical(jse_ticker, 30) if jse_ticker else None
        usd_1m    = fetch_historical("ZAR=X", 30)
        eur_1m    = fetch_historical("EURZAR=X", 30)
        gbp_1m    = fetch_historical("GBPZAR=X", 30)
        brent_1m  = fetch_historical("BZ=F", 30)
        gold_1m   = fetch_historical("GC=F", 30)
        sp_1m     = fetch_historical("^GSPC", 30)
        btc_1m    = fetch_bitcoin_historical(cg, 30)

        # YTD prices
        jse_ytd   = get_ytd_reference_price(jse_ticker) if jse_ticker else None
        usd_ytd   = get_ytd_reference_price("ZAR=X")
        eur_ytd   = get_ytd_reference_price("EURZAR=X")
        gbp_ytd   = get_ytd_reference_price("GBPZAR=X")
        brent_ytd = get_ytd_reference_price("BZ=F")
        gold_ytd  = get_ytd_reference_price("GC=F")
        sp_ytd    = get_ytd_reference_price("^GSPC")
        btc_ytd   = get_bitcoin_ytd_price(cg)

        # Assemble result
        return {
            "timestamp": report_time.strftime("%Y-%m-%d %H:%M"),
            "JSEALSHARE": {"Today": jse, "Change": calculate_percentage(jse_1d, jse), "Monthly": calculate_percentage(jse_1m, jse), "YTD": calculate_percentage(jse_ytd, jse)},
            "USDZAR":     {"Today": usd_zar, "Change": calculate_percentage(usd_1d, usd_zar), "Monthly": calculate_percentage(usd_1m, usd_zar), "YTD": calculate_percentage(usd_ytd, usd_zar)},
            "EURZAR":     {"Today": eur_zar, "Change": calculate_percentage(eur_1d, eur_zar), "Monthly": calculate_percentage(eur_1m, eur_zar), "YTD": calculate_percentage(eur_ytd, eur_zar)},
            "GBPZAR":     {"Today": gbp_zar, "Change": calculate_percentage(gbp_1d, gbp_zar), "Monthly": calculate_percentage(gbp_1m, gbp_zar), "YTD": calculate_percentage(gbp_ytd, gbp_zar)},
            "BRENT":      {"Today": brent, "Change": calculate_percentage(brent_1d, brent), "Monthly": calculate_percentage(brent_1m, brent), "YTD": calculate_percentage(brent_ytd, brent)},
            "GOLD":       {"Today": gold, "Change": calculate_percentage(gold_1d, gold), "Monthly": calculate_percentage(gold_1m, gold), "YTD": calculate_percentage(gold_ytd, gold)},
            "SP500":      {"Today": sp500, "Change": calculate_percentage(sp_1d, sp500), "Monthly": calculate_percentage(sp_1m, sp500), "YTD": calculate_percentage(sp_ytd, sp500)},
            "BITCOINZAR": {"Today": bitcoin, "Change": calculate_percentage(btc_1d, bitcoin), "Monthly": calculate_percentage(btc_1m, bitcoin), "YTD": calculate_percentage(btc_ytd, bitcoin)},
        }
    except Exception as e:
        print(f"❌ Critical error in fetch_market_data: {e}")
        return None
