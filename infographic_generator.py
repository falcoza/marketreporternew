def generate_infographic(data):
    # Font loading (keep previous)
    
    # New dimensions: 450x580 (reduced height)
    img = Image.new("RGB", (450, 580), THEME['background'])
    draw = ImageDraw.Draw(img)

    # Header
    draw.text((10, 10), f"Market Report {data['timestamp']}", 
             font=georgia_bold, fill=THEME['text'])

    # Table headers
    y = 50
    x = 10
    for col, width in REPORT_COLUMNS:
        draw.rectangle([x, y, x+width, y+30], fill=THEME['header'])
        txt_width = georgia_bold.getlength(col)
        draw.text((x + (width-txt_width)//2, y+5), col,  # Center-align
                 font=georgia_bold, fill="white")
        x += width

    # Data rows (adjusted spacing)
    y = 80
    for idx, (name, values) in enumerate(metrics):
        x = 10
        # Metric name (truncate if needed)
        draw.text((x+5, y+5), name, font=georgia, fill=THEME['text'])
        x += REPORT_COLUMNS[0][1]

        # Values
        for col in ["Today", "Change", "Monthly", "YTD"]:
            value = values[col]
            if col == "Today":
                text = f"{value:,.0f}" if value > 1000 else f"{value:,.2f}"
                color = THEME['text']
            else:
                text = f"{value:+.1f}%".replace("+0.0%", "0.0%")
                color = THEME['positive'] if value >= 0 else THEME['negative']
            
            txt_width = georgia.getlength(text)
            draw.text((x + (REPORT_COLUMNS[1][1]-txt_width)//2, y+5),  # Center-align
                     text, font=georgia, fill=color)
            x += REPORT_COLUMNS[1][1] if col == "Today" else REPORT_COLUMNS[2][1]

        y += 34  # Reduced row height

    # Compact footer
    footer = "Source: Yahoo Finance, CoinGecko"
    footer_font = ImageFont.truetype(FONT_PATHS['georgia'], 14)
    draw.text((10, 545), footer, font=footer_font, fill="#444444")

    img.save(filename)
    return filename
