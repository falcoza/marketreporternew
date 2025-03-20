def fetch_market_data():
    # ... (previous code)
    
    # Bitcoin handling
    try:
        bitcoin = cg.get_price(ids="bitcoin", vs_currencies="zar")["bitcoin"]["zar"]
        btc_hist = cg.get_coin_market_chart_by_id(
            id="bitcoin", 
            vs_currency="zar", 
            days=30
        )["prices"]
    except Exception as e:
        print(f"⚠️ Bitcoin data error: {str(e)}")
        bitcoin = 0
        btc_hist = []

    return {
        # ... (other metrics)
        "Bitcoin (ZAR)": {
            "Today": bitcoin,
            "Change": calculate_percentage(
                btc_hist[0][1] if len(btc_hist) > 1 else 0, 
                bitcoin
            ),
            "Monthly": calculate_percentage(
                btc_hist[0][1] if len(btc_hist) > 1 else 0, 
                bitcoin
            ),
            "YTD": calculate_percentage(
                get_bitcoin_ytd_start_price(), 
                bitcoin
            )
        }
    }

def get_bitcoin_ytd_start_price():
    """Get Bitcoin price from January 1st"""
    cg = CoinGeckoAPI()
    try:
        year_start = datetime(datetime.now().year, 1, 1).timestamp()
        history = cg.get_coin_market_chart_range_by_id(
            id="bitcoin",
            vs_currency="zar",
            from_timestamp=int(year_start),
            to_timestamp=int(year_start) + 86400
        )
        return history["prices"][0][1] if history["prices"] else 0
    except Exception as e:
        print(f"⚠️ Bitcoin YTD error: {str(e)}")
        return 0
