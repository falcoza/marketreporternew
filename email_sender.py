import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime
from config import EMAIL_SENDER, EMAIL_PASSWORD, EMAIL_RECEIVERS, SMTP_SERVER, SMTP_PORT

def send_report_email(filename):
    try:
        # Create email message
        msg = MIMEMultipart()
        msg["From"] = EMAIL_SENDER
        msg["To"] = ", ".join(EMAIL_RECEIVERS)  # Show all recipients in header
        msg["Subject"] = f"Market Report {datetime.now().strftime('%Y-%m-%d %H:%M')}"

        # Add body text
        body = MIMEText("Attached is the latest market report.", "plain")
        msg.attach(body)

        # Attach report file
        with open(filename, "rb") as file:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(file.read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f"attachment; filename={filename}")
        msg.attach(part)

        # Send email to all recipients
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.sendmail(
                EMAIL_SENDER,
                EMAIL_RECEIVERS,  # Send to all addresses in the list
                msg.as_string()
            )
        return True

    except Exception as e:
        print(f"❌ Email Error: {str(e)}")
        return False
