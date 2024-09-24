import os
from utils.helpers import download_file, extract_zip_file, convert_txt_to_csv, append_address_to_csv

# URL of the MTA static data zip file
MTA_DATA_URL = 'http://web.mta.info/developers/files/google_transit_supplemented.zip'

# geocoding URL
GEOCODING_URL = "https://nominatim.openstreetmap.org/reverse?lat={lat}&lon={lon}&format=json&addressdetails=1"

# Directory to store the downloaded text files
DATA_DIR = './mta_static_data/text_files/'

# Directory to store converted CSV files
CSV_DIR = './mta_static_data/csv_files'

# Path to the stops.csv file
STOPS_CSV = os.path.join(CSV_DIR, 'stops.csv')

if __name__ == '__main__':
    # Ensure the text files directory exists
    os.makedirs(DATA_DIR, exist_ok=True)
    
    # Set the path to save the downloaded zip file
    zip_path = os.path.join(DATA_DIR, 'mta_static_data.zip')

    # Download the MTA static data zip file
    download_file(MTA_DATA_URL, zip_path)

    # Extract the zip file to the text_files directory
    extract_zip_file(zip_path, DATA_DIR)

    # Remove the zip file after extraction to save space
    os.remove(zip_path)

    # Convert .txt files in the text_files directory to .csv files in the csv_files directory
    convert_txt_to_csv(DATA_DIR, CSV_DIR)
    
    # Append address information to the stops.csv file
    append_address_to_csv(STOPS_CSV, STOPS_CSV, GEOCODING_URL)
