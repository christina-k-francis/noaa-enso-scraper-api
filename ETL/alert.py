"""
Email alert system for detecting new ONI values once they are published
on the NOAA NWS CPC website. Sends an email when new data is detected.
"""

import os
import smtplib
import requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from bs4 import BeautifulSoup
from prefect import task, flow, get_run_logger
from datetime import datetime
from load import get_from_r2

@task(name="fetch-noaa-latest-oni", retries=2)
def fetch_latest_oni_from_noaa():
    """
    description:
        Thix FX scrapes the most recent ONI value from the NOAA website.
    output:
        The latest year, season, and ONI value from NOAA.
    """
    url = "https://cpc.ncep.noaa.gov/products/analysis_monitoring/ensostuff/ONI_v5.php"
    page = requests.get(url) # Response 200 indicates we are permitted collect data from this website 
    # obtain page's information 
    soup = BeautifulSoup(page.text, 'lxml') 
    # obtain information from the webpage's primary table: tag <table align="center" border="1"> 
    main_table = soup.find('table', align='center', border='1') 
    # getting seasons from the table header
    seasons = [h.text for h in main_table.find_all('strong')][1:13]

    # get all rows in the main table
    rows = main_table.find_all('tr')
    # find the last row in the table
    row = rows[-1]
    # get the row's year value
    year_cell = row.find('strong')
    if year_cell:
        year_value = int(year_cell.text)
        cells = row.find_all('td', style="text-align:center;", width="7%")
        # Find the last cell with data in this row
        for cell_idx in range(len(cells) - 1, 0, -1):
            cell = cells[cell_idx]
            # obtain relevant information
            oni_value = float(cell.find('span').text)
            season = seasons[cell_idx] if cell_idx < len(seasons) else None
            return {'year': year_value,
                    'season': season,
                    'oni': oni_value,
                    'timestamp': datetime.utcnow().isoformat()}    
    else:
        return None


@task(name="get-stored-latest-oni", retries=2)
def get_stored_latest_oni():
    """
    description:
        Get the most recent ONI value from the enhanced_oni_latest parquet 
        file in R2 storage
    """
    # retrieving the enhanced ONI dataset
    df = get_from_r2()

    # Get most recent non-null ONI value
    df = df[df['ONI'].notna()].sort_values(['year', 'season'])
    latest = df.iloc[-1]
    
    return {
        'year': int(latest['year']),
        'season': latest['season'],
        'oni': float(latest['ONI']),
    }


@task(name="send-email-alert", retries=2)
def send_email_alert(new_data, old_data=None):
    """
    description:
        This FX sends email notification about new ONI data,
        if there is new data found.
    """
    logger = get_run_logger()
    
    sender_email = os.environ.get("EMAIL_SENDER")
    sender_password = os.environ.get("EMAIL_PASSWORD")
    recipient_email = os.environ.get("EMAIL_RECIPIENT")
    
    if not all([sender_email, sender_password, recipient_email]):
        logger.warning("Email credentials not set. Skipping email notification.")
        return False
    
    # Create email content
    subject = f"New ONI Data added by NOAA: {new_data['season']} {new_data['year']}"
    
    # Determine ENSO phase
    oni = new_data['oni']
    if oni >= 0.5:
        phase = "El Niño"
    elif oni <= -0.5:
        phase = "La Niña"
    else:
        phase = "Neutral"
    
    body = f"""
    New NOAA ONI Value Detected!
    
    ENSO Phase: {phase}
    Season: {new_data['season']} {new_data['year']}
    ONI Value: {oni:.2f}
    Detected at {new_data.get('timestamp', 'N/A')}UTC
    """
    
    if old_data:
        # checking ENSO phase
        old_oni = old_data['oni']
        if old_oni >= 0.5:
            old_phase = "El Niño"
        elif old_oni <= -0.5:
            old_phase = "La Niña"
        else:
            old_phase = "Neutral"

        body += f"""
    Latest Stored ONI Data:

    ENSO Phase: {old_phase}
    Season: {old_data['season']} {old_data['year']}
    ONI Value: {old_data['oni']:.2f}
    SSTA Change: {oni - old_data['oni']:.2f}
    """
    
    body += """Automated alert from NOAA ENSO Scraper"""
    
    # Create message
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = recipient_email
    message["Subject"] = subject
    message.attach(MIMEText(body, "plain"))
    
    # Send email
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender_email, sender_password)
            server.send_message(message)
        
        logger.info(f"Alert email sent to {recipient_email}")
        return True
    
    except Exception as e:
        logger.error(f"Failed to send email: {e}")
        return False


@flow(name="check-for-new-oni-data", retries=1)
def check_for_new_oni_data():
    """
    description:
        The main flow that checks if NOAA posts new data, and sends an
        email alert if so.
    """
    logger = get_run_logger()
    
    logger.info("Checking NOAA website for new ONI data...")
    noaa_latest = fetch_latest_oni_from_noaa()
    
    if not noaa_latest:
        logger.warning("Could not fetch latest NOAA data")
        return {"status": "error", "message": "Failed to fetch NOAA data"}
    
    logger.info(f"NOAA latest: {noaa_latest['season']} {noaa_latest['year']} = {noaa_latest['oni']}")
    
    logger.info("Checking stored data...")
    stored_latest = get_stored_latest_oni()
    
    if not stored_latest:
        logger.warning("Could not fetch stored data - assuming this is new")
        send_email_alert(noaa_latest)
        return {"status": "new_data", "data": noaa_latest}
    
    logger.info(f"Stored latest: {stored_latest['season']} {stored_latest['year']} = {stored_latest['oni']}")
    
    # Check if NOAA data is newer
    is_new = (
        noaa_latest['year'] > stored_latest['year'] or
        (noaa_latest['year'] == stored_latest['year'] and 
         noaa_latest['season'] != stored_latest['season'])
    )
    
    if is_new:
        logger.info("NEW DATA DETECTED! Sending alert...")
        send_email_alert(noaa_latest, stored_latest)
        return {
            "status": "new_data",
            "new": noaa_latest,
            "previous": stored_latest
        }
    else:
        logger.info("No new data - stored data is current")
        return {"status": "up_to_date", "latest": noaa_latest}