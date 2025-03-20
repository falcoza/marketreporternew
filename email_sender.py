def send_report_email(filename):
    """Send email with report attachment"""
    msg = MIMEMultipart()
    msg["From"] = EMAIL_SENDER
    msg["To"] = EMAIL_RECEIVER
    msg["Subject"] = f"Market Report {datetime.now().strftime('%Y-%m-%d %H:%M')}"

    # Add body text
    body = "Please find attached the latest market report."
    msg.attach(MIMEText(body, "plain"))

    # Attach report
    try:
        with open(filename, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header(
            "Content-Disposition",
            f"attachment; filename= {filename}",
        )
        msg.attach(part)
    except FileNotFoundError:
        raise ValueError(f"Report file {filename} not found")

    # Send email
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        server.sendmail(EMAIL_SENDER, EMAIL_RECEIVER, msg.as_string())
