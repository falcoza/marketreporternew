from data_fetcher import fetch_market_data
from infographic_generator import generate_infographic
from email_sender import send_report_email

def main():
    print("🚀 Starting market report generation...")
    
    # Fetch data
    market_data = fetch_market_data()
    if not market_data:
        print("❌ Failed to fetch data")
        return

    # Generate infographic
    try:
        filename = generate_infographic(market_data)
        print(f"✅ Generated: {filename}")
    except Exception as e:
        print(f"❌ Infographic failed: {str(e)}")
        return

    # Send email
    if send_report_email(filename):
        print("✅ Report sent successfully!")
    else:
        print("❌ Failed to send email")

if __name__ == "__main__":
    main()
