import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime
from config import *

def send_report_email(filename):
    """Enhanced email sender with detailed error handling"""
    msg = MIMEMultipart()
    msg["From"] = EMAIL_SENDER
    msg["To"] = EMAIL_RECEIVER
    msg["Subject"] = f"Market Report {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    
    try:
        # Create email body
        body = MIMEText("Attached is the latest market report.", "plain")
        msg.attach(body)

        # Attach report
        with open(filename, "rb") as f:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f"attachment; filename={filename}")
        msg.attach(part)

        # SMTP connection with debugging
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.set_debuglevel(1)  # Enable verbose logging
            server.starttls()
            
            # Validate credentials before sending
            if not EMAIL_PASSWORD:
                raise ValueError("Email password not configured")
                
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.send_message(msg)
            
        return True
        
    except smtplib.SMTPAuthenticationError as e:
        print(f"❌ SMTP Authentication Failed: {str(e)}")
        print("Verify: 1. Gmail App Password 2. 2FA Enabled 3. Sender Email Match")
        return False
        
    except Exception as e:
        print(f"❌ Email Error: {str(e)}")
        return False
