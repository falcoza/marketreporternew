import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from config import EMAIL_SENDER, EMAIL_PASSWORD, EMAIL_RECEIVER, SMTP_SERVER, SMTP_PORT

def send_report_email():
    """Send email with attachment using secure connection"""
    if not EMAIL_PASSWORD:
        raise ValueError("Email password not configured")

    msg = MIMEMultipart()
    msg["From"] = EMAIL_SENDER
    msg["To"] = EMAIL_RECEIVER
    msg["Subject"] = "Daily Market Report"

    body = "Attached is today's automated market report"
    msg.attach(MIMEText(body, "plain"))

    try:
        with open("market_report.png", "rb") as f:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", "attachment; filename=market_report.png")
            msg.attach(part)
    except FileNotFoundError:
        raise ValueError("Report file not found")

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.sendmail(EMAIL_SENDER, EMAIL_RECEIVER, msg.as_string())
        return True
    except smtplib.SMTPAuthenticationError:
        raise ValueError("SMTP authentication failed - check credentials")
