now = datetime.now(timezone.utc)
target_date = now - timedelta(days=days)
window = timedelta(hours=12)
        history = cg.get_coin_market_chart_range_by_id("bitcoin", "zar", int((target_date - window).timestamp()), int((target_date + window).timestamp()))
        history = cg.get_coin_market_chart_range_by_id("bitcoin", "zar",
                                                       int((target_date - window).timestamp()),
                                                       int((target_date + window).timestamp()))
prices = history.get("prices", [])
if not prices:
return None
@@ -64,12 +66,46 @@ def get_bitcoin_ytd_price(cg: CoinGeckoAPI) -> Optional[float]:
current_year = datetime.now(timezone.utc).year
start_date = datetime(current_year, 1, 1, tzinfo=timezone.utc)
end_date = start_date + timedelta(days=1)
        history = cg.get_coin_market_chart_range_by_id("bitcoin", "zar", int(start_date.timestamp()), int(end_date.timestamp()))
        history = cg.get_coin_market_chart_range_by_id("bitcoin", "zar",
                                                       int(start_date.timestamp()),
                                                       int(end_date.timestamp()))
return history['prices'][0][1] if history.get('prices') else None
except Exception as e:
print(f"⚠️ Bitcoin YTD error: {str(e)}")
return None

def fetch_paxg_historical_price(cg: CoinGeckoAPI, days: int) -> Optional[float]:
    try:
        now = datetime.now(timezone.utc)
        target_date = now - timedelta(days=days)
        window = timedelta(hours=12)
        from_timestamp = int((target_date - window).timestamp())
        to_timestamp = int((target_date + window).timestamp())
        history = cg.get_coin_market_chart_range_by_id("pax-gold", "zar", from_timestamp, to_timestamp)
        prices = history.get("prices", [])
        if not prices:
            return None
        target_ts = target_date.timestamp() * 1000
        closest_price = min(prices, key=lambda x: abs(x[0] - target_ts))
        return closest_price[1]
    except Exception as e:
        print(f"⚠️ PAXG historical fetch error for {days} days: {str(e)}")
        return None

def fetch_paxg_ytd_price(cg: CoinGeckoAPI) -> Optional[float]:
    try:
        current_year = datetime.now(timezone.utc).year
        start_date = datetime(current_year, 1, 1, tzinfo=timezone.utc)
        end_date = start_date + timedelta(days=1)
        history = cg.get_coin_market_chart_range_by_id("pax-gold", "zar",
                                                       int(start_date.timestamp()),
                                                       int(end_date.timestamp()))
        prices = history.get("prices", [])
        return prices[0][1] if prices else None
    except Exception as e:
        print(f"⚠️ PAXG YTD price error: {str(e)}")
        return None

def get_latest_price(ticker: str) -> Optional[float]:
try:
stock = yf.Ticker(ticker)
@@ -160,9 +196,9 @@ def fetch_market_data() -> Optional[Dict[str, Any]]:
},
"GOLD": {
"Today": gold_now,
                "Change": calculate_percentage(fetch_bitcoin_historical(cg, 1), gold_now),  # placeholder
                "Monthly": calculate_percentage(fetch_bitcoin_historical(cg, 30), gold_now),  # placeholder
                "YTD": calculate_percentage(get_bitcoin_ytd_price(cg), gold_now)  # placeholder
                "Change": calculate_percentage(fetch_paxg_historical_price(cg, 1), gold_now),
                "Monthly": calculate_percentage(fetch_paxg_historical_price(cg, 30), gold_now),
                "YTD": calculate_percentage(fetch_paxg_ytd_price(cg), gold_now)
},
"SP500": {
"Today": indices["^GSPC"],
