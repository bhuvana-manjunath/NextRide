import requests
import zipfile
import os
import csv
import pandas as pd
from google.transit import gtfs_realtime_pb2
from datetime import datetime
import logging

# Configure logging to display informational messages with timestamps
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def download_file(url, destination_path):
    """
    Downloads a file from the specified URL and saves it at the given path.

    Parameters:
    url (str): The URL to download the file from.
    destination_path (str): The path where the downloaded file will be stored.

    Returns:
    None
    """
    logger.info(f"Initiating download from {url}...")
    response = requests.get(url)
    
    if response.status_code == 200:
        with open(destination_path, 'wb') as file:
            file.write(response.content)
        logger.info(f"File downloaded and saved to {destination_path}")
    else:
        logger.error(f"Download failed with status code: {response.status_code}")

def extract_zip_file(zip_file_path, extract_dir):
    """
    Extracts the contents of a zip file to a specified directory.

    Parameters:
    zip_file_path (str): The path of the zip file to be extracted.
    extract_dir (str): The directory where the extracted contents will be stored.

    Returns:
    None
    """
    logger.info(f"Extracting {zip_file_path} to {extract_dir}...")
    
    try:
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
        logger.info("Extraction completed successfully.")
    except zipfile.BadZipFile as e:
        logger.error(f"Zip file extraction failed: {e}")

def convert_txt_to_csv(input_dir, output_dir):
    """
    Converts all .txt files in the input directory to .csv format and saves them in the output directory.

    Parameters:
    input_dir (str): Directory containing .txt files.
    output_dir (str): Directory where the converted .csv files will be saved.

    Returns:
    None
    """
    if not os.path.isdir(input_dir):
        logger.error(f"Input directory '{input_dir}' not found.")
        return

    os.makedirs(output_dir, exist_ok=True)

    for filename in os.listdir(input_dir):
        if filename.endswith(".txt"):
            txt_file_path = os.path.join(input_dir, filename)
            csv_file_path = os.path.join(output_dir, filename.replace(".txt", ".csv"))
            
            try:
                with open(txt_file_path, 'r') as txt_file:
                    reader = csv.reader(txt_file, delimiter=',', quotechar='"')

                    with open(csv_file_path, 'w', newline='') as csv_file:
                        writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                        for row in reader:
                            writer.writerow(row)
                logger.info(f"Converted {txt_file_path} to {csv_file_path}")
            except Exception as e:
                logger.error(f"Error during conversion of {txt_file_path}: {e}")

def fetch_reverse_geocoding(lat, lon, cache, api_url):
    """
    Fetches the address for given latitude and longitude using a reverse geocoding API.
    Results are cached to avoid redundant API calls.

    Parameters:
    lat (float): Latitude of the location.
    lon (float): Longitude of the location.
    cache (dict): Cache to store fetched addresses.
    api_url (str): Reverse geocoding API URL template.

    Returns:
    str: The address (suburb and postcode) or 'Address not found' if unsuccessful.
    """
    cache_key = (lat, lon)
    
    if cache_key in cache:
        return cache[cache_key]

    formatted_url = api_url.format(lat=lat, lon=lon)
    headers = {'User-Agent': 'Bhuvana'}
    response = requests.get(formatted_url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        address = data.get('address', {})
        suburb = address.get('suburb', '')
        postcode = address.get('postcode', '')
        result = f"{suburb}, {postcode}" if suburb or postcode else 'Address not found'
    else:
        result = 'Address not found'
        logger.warning(f"Failed to fetch address for coordinates {lat}, {lon} - Status Code: {response.status_code}")

    cache[cache_key] = result
    return result

def append_address_to_csv(input_csv, output_csv, geocoding_api_url):
    """
    Adds an 'address' column to a CSV file based on 'stop_lat' and 'stop_lon' columns 
    by using a reverse geocoding API.

    Parameters:
    input_csv (str): Path to the input CSV file.
    output_csv (str): Path to save the updated CSV file with address information.
    geocoding_api_url (str): URL template for the reverse geocoding API.

    Returns:
    None
    """
    try:
        df = pd.read_csv(input_csv)

        if 'stop_lat' not in df.columns or 'stop_lon' not in df.columns:
            logger.error("Input CSV must contain 'stop_lat' and 'stop_lon' columns.")
            return

        cache = {}
        df['address'] = df.apply(lambda row: fetch_reverse_geocoding(row['stop_lat'], row['stop_lon'], cache, geocoding_api_url), axis=1)
        df.to_csv(output_csv, index=False)
        logger.info(f"Address data appended successfully. Updated CSV saved to {output_csv}")
    except Exception as e:
        logger.error(f"Failed to append address data to CSV: {e}")

def fetch_gtfs_realtime_data(gtfs_url):
    """
    Retrieves GTFS-Realtime data from the specified MTA GTFS feed URL.

    Parameters:
    gtfs_url (str): URL of the GTFS feed.

    Returns:
    gtfs_realtime_pb2.FeedMessage: Parsed FeedMessage object containing GTFS data, or None if an error occurs.
    """
    feed = gtfs_realtime_pb2.FeedMessage()

    try:
        response = requests.get(gtfs_url)
        response.raise_for_status()
        feed.ParseFromString(response.content)
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch GTFS-Realtime data: {e}")
        return None

    return feed

def process_gtfs_feed_data(feed_list):
    """
    Processes GTFS-Realtime feed data to extract trip updates, stop time updates, and alerts.

    Parameters:
    feed_list (list): List of GTFS FeedMessage objects.

    Returns:
    tuple: Three lists containing:
           1. Trip updates (trip_id, start_datetime, route_id)
           2. Stop time updates (trip_id, stop_id, arrival_time, departure_time)
           3. Alerts (alert_id, header_text, description_text, active_periods, informed_entities)
    """
    trip_updates = []
    stop_time_updates = []
    alerts = []

    try:
        for feed in feed_list:
            for entity in feed.entity:
                if entity.HasField('trip_update'):
                    trip_id = entity.trip_update.trip.trip_id
                    start_time = entity.trip_update.trip.start_time
                    start_date = entity.trip_update.trip.start_date
                    route_id = entity.trip_update.trip.route_id

                    start_datetime = None
                    if start_time and start_date:
                        start_datetime_str = f"{start_date} {start_time}"
                        start_datetime = datetime.strptime(start_datetime_str, '%Y%m%d %H:%M:%S')

                    trip_updates.append((trip_id, start_datetime, route_id))

                    for stop_time in entity.trip_update.stop_time_update:
                        stop_id = stop_time.stop_id
                        arrival_time = datetime.fromtimestamp(stop_time.arrival.time) if stop_time.HasField('arrival') else None
                        departure_time = datetime.fromtimestamp(stop_time.departure.time) if stop_time.HasField('departure') else None
                        stop_time_updates.append((trip_id, stop_id, arrival_time, departure_time))

                if entity.HasField('alert'):
                    alert_id = entity.id
                    header_text = entity.alert.header_text.translation[0].text if entity.alert.header_text.translation else None
                    description_text = entity.alert.description_text.translation[0].text if entity.alert.description_text.translation else None

                    active_periods = [
                        (datetime.fromtimestamp(period.start) if period.HasField('start') else None,
                         datetime.fromtimestamp(period.end) if period.HasField('end') else None)
                        for period in entity.alert.active_period
                    ]

                    informed_entities = [
                        (entity.agency_id if entity.HasField('agency_id') else None,
                         entity.route_id if entity.HasField('route_id') else None,
                         entity.stop_id if entity.HasField('stop_id') else None)
                        for entity in entity.alert.informed_entity
                    ]

                    alerts.append({
                        'alert_id': alert_id,
                        'header_text': header_text,
                        'description_text': description_text,
                        'active_periods': active_periods,
                        'informed_entities': informed_entities
                    })

    except Exception as e:
        logger.error(f"Error processing GTFS feed data: {e}")

    return trip_updates, stop_time_updates, alerts
