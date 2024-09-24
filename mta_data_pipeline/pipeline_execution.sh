#!/bin/bash

# Download MTA static data, extract the ZIP file, convert text files to CSV, and add address column to stops file
# This script calls a Python script that performs multiple tasks:
# 1. Creates directories for storing text and CSV files.
# 2. Downloads the MTA static data ZIP file.
# 3. Extracts the contents of the ZIP file into the specified directory.
# 4. Converts all text files to CSV format.
# 5. Adds an address column to the 'stops.csv' file based on latitude and longitude.
python3 -m mta_data_pipeline.download_static_data

# Upload all CSV files to the PostgreSQL database
# This script reads the CSV files from the specified directory and uploads their contents to PostgreSQL tables
python3 -m mta_data_pipeline.create_schema

# Add a new column 'trains' to the 'stops' table in the PostgreSQL database
# This script updates the database schema by adding a column and populates it with route information
python3 -m mta_data_pipeline.upload_static_data_to_postgres