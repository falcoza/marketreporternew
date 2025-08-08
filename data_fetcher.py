one_month_ago = (now - timedelta(days=33)).strftime('%Y-%m-%d')  # 3-day buffer
ytd_start = datetime(now.year, 1, 1).strftime('%Y-%m-%d')

        # Yahoo tickers with fallback symbols
        # Yahoo tickers
tickers = {
"JSEALSHARE": "^J203.JO",
"USDZAR": "USDZAR=X",
@@ -50,7 +50,6 @@ def fetch_market_data() -> Optional[Dict[str, Any]]:
}

data = {}
        fallback_values = {}

for label, symbol in tickers.items():
try:
@@ -65,69 +64,59 @@ def fetch_market_data() -> Optional[Dict[str, Any]]:
today_val = daily_hist["Close"].iloc[-1]
day_ago_val = daily_hist["Close"].iloc[-2] if len(daily_hist) > 1 else today_val

                # Get monthly data with wider window
                # Get monthly data
monthly_hist = ticker.history(start=one_month_ago, end=one_day_ago)
                if not monthly_hist.empty:
                    month_ago_val = monthly_hist["Close"].iloc[0]
                else:
                    # Fallback to approximate monthly calculation
                    month_ago_val = today_val / (1 + (fallback_values.get('avg_monthly_change', 0.01) if label in fallback_values 
                                                    else 0.01))
                month_ago_val = monthly_hist["Close"].iloc[0] if not monthly_hist.empty else None

                # Get YTD data with validation
                # Get YTD data
ytd_hist = ticker.history(start=ytd_start)
ytd_val = ytd_hist["Close"].iloc[0] if not ytd_hist.empty else None

                # Validate data sanity
                if ytd_val and abs(calculate_percentage(ytd_val, today_val)) > 300:  # Sanity check
                    ytd_val = None

data[label] = {
"Today": float(today_val),
"Change": calculate_percentage(day_ago_val, today_val),
"Monthly": calculate_percentage(month_ago_val, today_val),
"YTD": calculate_percentage(ytd_val, today_val) if ytd_val else 0.0
}

                # Store for potential fallback calculations
                fallback_values[label] = {
                    'last_value': today_val,
                    'avg_monthly_change': data[label]["Monthly"] / 100 if data[label]["Monthly"] else 0.01
                }

except Exception as e:
print(f"⚠️ Error fetching {label}: {str(e)}")
continue

        # Crypto from CoinGecko with enhanced error handling
        # Crypto from CoinGecko with simplified matching
try:
cg = CoinGeckoAPI()
btc_data = cg.get_coin_market_chart_range_by_id(
id='bitcoin',
vs_currency='zar',
                from_timestamp=int((datetime(now.year, 1, 1) - timedelta(days=1)).timestamp()),
                to_timestamp=int((now + timedelta(days=1)).timestamp())
                from_timestamp=int(datetime.strptime(ytd_start, "%Y-%m-%d").timestamp()),
                to_timestamp=int(now.timestamp())
)

prices = btc_data.get('prices', [])
if not prices:
raise ValueError("Empty BTC price data")

            # Find nearest prices to avoid exact date matching issues
btc_today = prices[-1][1]
            target_day = (now - timedelta(days=1)).date()
            btc_day_ago = min(
                (p[1] for p in prices if abs((datetime.fromtimestamp(p[0]/1000).date() - target_day).days <= 1),
                key=lambda x: abs(x - btc_today * 0.98)  # Allow 2% variance
            
            # Simplified date matching
            btc_day_ago = next(
                (p[1] for p in prices 
                if datetime.fromtimestamp(p[0]/1000).date() == (now - timedelta(days=1)).date()),
                prices[-2][1]  # Fallback to previous data point
)

            target_month = (now - timedelta(days=30)).date()
            btc_month_ago = min(
                (p[1] for p in prices if abs((datetime.fromtimestamp(p[0]/1000).date() - target_month).days <= 3),
                key=lambda x: abs(x - btc_today * 0.85)  # Allow 15% variance
            btc_month_ago = next(
                (p[1] for p in prices 
                if datetime.fromtimestamp(p[0]/1000).date() == (now - timedelta(days=30)).date()),
                prices[0][1]  # Fallback to earliest data point
)

            btc_ytd = next((p[1] for p in prices if datetime.fromtimestamp(p[0]/1000).date() >= datetime(now.year, 1, 1).date()), None)
            btc_ytd = next(
                (p[1] for p in prices 
                if datetime.fromtimestamp(p[0]/1000).date() >= datetime(now.year, 1, 1).date()),
                prices[0][1]
            )

data["BITCOINZAR"] = {
"Today": float(btc_today),
@@ -138,20 +127,15 @@ def fetch_market_data() -> Optional[Dict[str, Any]]:

except Exception as e:
print(f"⚠️ Error fetching BTC data: {str(e)}")
            # Provide fallback BTC data if available
            if 'BITCOINZAR' in data:
                print("⚠️ Using cached BTC data due to API error")
            else:
                data["BITCOINZAR"] = {
                    "Today": 0.0,
                    "Change": 0.0,
                    "Monthly": 0.0,
                    "YTD": 0.0
                }
            data["BITCOINZAR"] = {
                "Today": 0.0,
                "Change": 0.0,
                "Monthly": 0.0,
                "YTD": 0.0
            }

        # Add timestamp and metadata
        # Add timestamp
data["timestamp"] = now.strftime("%d %b %Y, %H:%M")
        data["data_status"] = "complete" if all(k in data for k in tickers) else "partial"

return data
