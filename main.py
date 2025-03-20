from email_sender import send_report_email  # ✅ Correct import

def main():
    print("🚀 Starting market report generation...")
    
    # Step 1: Fetch data
    market_data = fetch_market_data()
    if not market_data:
        print("❌ Failed to fetch market data")
        return

    # Step 2: Generate infographic
    try:
        filename = generate_infographic(market_data)
        print(f"✅ Generated: {filename}")
    except Exception as e:
        print(f"❌ Infographic failed: {str(e)}")
        return

    # Step 3: Send email
    try:
        send_report_email(filename)  # ✅ Correct function call
        print("✅ Report successfully sent!")
    except Exception as e:
        print(f"❌ Email failed: {str(e)}")

if __name__ == "__main__":
    main()
