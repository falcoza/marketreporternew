# Add this right before the timestamp code
try:
    cg = CoinGeckoAPI()
    btc_data = cg.get_coin_market_chart_range_by_id(
        id='bitcoin',
        vs_currency='zar',
        from_timestamp=int(ytd_start.timestamp()),
        to_timestamp=int(now.timestamp())
    )
    
    if btc_data.get('prices'):
        prices = sorted(btc_data['prices'], key=lambda x: x[0])
        btc_today = prices[-1][1]
        
        def find_closest_price(target_date):
            return min(
                (p[1] for p in prices),
                key=lambda x: abs(datetime.fromtimestamp(p[0]/1000).date() - target_date)
            )
        
        data["BITCOINZAR"] = {
            "Today": float(btc_today),
            "Change": calculate_percentage(find_closest_price(now.date() - timedelta(days=1)), btc_today),
            "Monthly": calculate_percentage(find_closest_price(now.date() - timedelta(days=30)), btc_today),
            "YTD": calculate_percentage(prices[0][1], btc_today)
        }
except Exception as e:
    print(f"⚠️ BTC fetch failed: {str(e)}")
    data["BITCOINZAR"] = {
        "Today": 0.0,
        "Change": 0.0,
        "Monthly": 0.0,
        "YTD": 0.0
    }
