import os
import pandas as pd
from utils.db import Database

# Directory containing CSV data
CSV_DIR = './mta_static_data/csv_files'

# SQL query to update the 'trains' column in the 'stops' table with a comma-separated list of route IDs for each stop
UPDATE_TRAINS_IN_STOPS_QUERY = """
WITH aggregated_routes AS (
    SELECT
        LEFT(stop_id, LENGTH(stop_id) - 1) AS base_id,  -- Extract base ID by removing the last character
        STRING_AGG(DISTINCT r.route_id, ', ') AS routes
    FROM stop_times st
    JOIN trips t ON st.trip_id = t.trip_id
    JOIN routes r ON t.route_id = r.route_id
    GROUP BY LEFT(stop_id, LENGTH(stop_id) - 1)  
)
UPDATE stops s
SET trains = subquery.routes
FROM aggregated_routes subquery
WHERE s.stop_id = subquery.base_id;

UPDATE stops s
SET trains = subquery.routes
FROM (
    SELECT st.stop_id, STRING_AGG(DISTINCT r.route_id, ', ') AS routes
    FROM stop_times st
    JOIN trips t ON st.trip_id = t.trip_id
    JOIN routes r ON t.route_id = r.route_id
    GROUP BY st.stop_id
) subquery
WHERE s.stop_id = subquery.stop_id;
"""

def upload_to_postgres(db, csv_dir):
    """
    Uploads static MTA data from CSV files to PostgreSQL and updates the 'trains' column in the 'stops' table.

    Parameters:
    db (Database): The database instance to interact with PostgreSQL.
    csv_dir (str): The directory containing the CSV files.
    """
    # Iterate over CSV files in the directory
    for csv_file in os.listdir(csv_dir):
        csv_path = os.path.join(csv_dir, csv_file)
        table_name = os.path.splitext(csv_file)[0]
        df = pd.read_csv(csv_path)
        columns = list(df.columns)
        
        # Prepare data for batch insert
        rows = [list(row) for _, row in df.iterrows()]  # Convert each row to a list of values
        
        # Insert data into the table using batch mode
        db.insert_data(table_name, columns, rows)
        
        print(f"Uploaded {csv_file} to the {table_name} table.")
    
    # After all data is uploaded, update the 'trains' column in the 'stops' table
    db.execute_sql_query(UPDATE_TRAINS_IN_STOPS_QUERY)
    print("Successfully updated the 'stops' table with route information.")

if __name__ == "__main__":
    # Initialize the Database connection pool
    db = Database()
    db.initialize_pool()
    
    try:
        # Upload MTA data and update the 'trains' column
        upload_to_postgres(db, CSV_DIR)
        print("MTA static data upload and processing completed successfully.")
    # except Exception as e:
    #     print(f"An error occurred: {e}")
    finally:
        db.close_all_connections
