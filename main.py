# main.py
from dotenv import load_dotenv
from data_fetcher import fetch_market_data
from infographic_generator import generate_infographic
from email_sender import send_report_email

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
        send_report_email(filename)
        print("✅ Report successfully sent!")
    except Exception as e:
        print(f"❌ Email failed: {str(e)}")

if __name__ == "__main__":
    load_dotenv()  # Load environment variables
    main()
