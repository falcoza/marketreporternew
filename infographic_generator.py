from PIL import Image, ImageDraw, ImageFont
from datetime import datetime
from config import *

def generate_infographic(data):
    try:
        # Load Georgia fonts with fallback
        georgia = ImageFont.truetype(FONT_PATHS['georgia'], 18)
        georgia_bold = ImageFont.truetype(FONT_PATHS['georgia_bold'], 20)
        footer_font = ImageFont.truetype(FONT_PATHS['georgia'], 16)

        # Create 520px wide canvas
        img = Image.new("RGB", (520, 550), THEME['background'])
        draw = ImageDraw.Draw(img)

        # Header Section
        header_text = f"Market Report {data['timestamp']}"
        header_width = georgia_bold.getlength(header_text)
        draw.text(
            ( (520 - header_width) // 2, 15 ),  # Center-aligned
            header_text,
            font=georgia_bold,
            fill=THEME['text']
        )

        # Table Headers
        y_position = 60
        x_position = 25
        for col_name, col_width in REPORT_COLUMNS:
            # Header background
            draw.rectangle(
                [(x_position, y_position), 
                 (x_position + col_width, y_position + 30)],
                fill=THEME['header']
            )
            
            # Header text (center-aligned)
            text_width = georgia_bold.getlength(col_name)
            draw.text(
                (x_position + (col_width - text_width) // 2, y_position + 5),
                col_name,
                font=georgia_bold,
                fill="white"
            )
            x_position += col_width

        # Data Rows
        y_position = 90
        metrics = [
            ("JSE All Share", data["JSEALSHARE"]),
            ("USD/ZAR", data["USDZAR"]),
            ("EUR/ZAR", data["EURZAR"]),
            ("GBP/ZAR", data["GBPZAR"]),
            ("Brent Crude", data["BRENT"]),
            ("Gold", data["GOLD"]),
            ("S&P 500", data["SP500"]),
            ("Bitcoin ZAR", data["BITCOINZAR"])
        ]

        for idx, (metric_name, values) in enumerate(metrics):
            x_position = 25
            bg_color = "#F5F5F5" if idx % 2 == 0 else THEME['background']
            
            # Row background
            draw.rectangle(
                [(25, y_position), (520 - 25, y_position + 34)],
                fill=bg_color
            )

            # Metric Name
            draw.text(
                (x_position + 5, y_position + 5),
                metric_name,
                font=georgia,
                fill=THEME['text']
            )
            x_position += REPORT_COLUMNS[0][1]  # Move to "Today" column

            # Today's Value
            today_val = values["Today"]
            today_text = f"{today_val:,.0f}" if today_val > 1000 else f"{today_val:,.2f}"
            draw.text(
                (x_position + 5, y_position + 5),
                today_text,
                font=georgia,
                fill=THEME['text']
            )
            x_position += REPORT_COLUMNS[1][1]

            # Percentage Values
            for period in ["Change", "Monthly", "YTD"]:
                value = values[period]
                color = THEME['positive'] if value >= 0 else THEME['negative']
                text = f"{value:+.1f}%"
                
                # Center-align in column
                text_width = georgia.getlength(text)
                draw.text(
                    (x_position + (REPORT_COLUMNS[2][1] - text_width) // 2, 
                    y_position + 5
                ),
                    text,
                    font=georgia,
                    fill=color
                )
                x_position += REPORT_COLUMNS[2][1]

            y_position += 34

        # Footer (bottom-right aligned)
        footer_text = "Data: Yahoo Finance, CoinGecko"
        footer_width = footer_font.getlength(footer_text)
        draw.text(
            (520 - footer_width - 15, 525),
            footer_text,
            font=footer_font,
            fill="#666666"
        )

        filename = f"Market_Report_{datetime.now().strftime('%Y%m%d_%H%M')}.png"
        img.save(filename)
        return filename

    except Exception as e:
        raise RuntimeError(f"Infographic generation failed: {str(e)}")
