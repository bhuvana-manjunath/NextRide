import os
import logging
import pandas as pd
from utils.db import Database

# Set up logging with INFO level to track the workflow and format logs with timestamp, level, and message
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Directory path where the CSV files containing static MTA data are stored
CSV_DIR = './mta_static_data/csv_files'

# Mapping of CSV file names to PostgreSQL table names for easy reference
CSV_TO_TABLE_MAP = {
    'agency.csv': 'agency',
    'routes.csv': 'routes',
    'trips.csv': 'trips',
    'stops.csv': 'stops',
    'stop_times.csv': 'stop_times',
    'calendar.csv': 'calendar',
    'calendar_dates.csv': 'calendar_dates',
    'shapes.csv': 'shapes',
    'transfers.csv': 'transfers',
}

# Mapping of table names to their primary key columns, if they have one
PRIMARY_KEYS = {
    'agency': 'agency_id',
    'calendar_dates': 'service_id',
    'calendar': 'service_id',
    'shapes': 'shape_id',
    'stops': 'stop_id',
    'routes': 'route_id',
}

# Real-time table schema definition for trip updates and stop-time updates
REAL_TIME_TABLES = {
    'trips_real_time': {
        'trip_id': {'type': 'VARCHAR(255)', 'constraints': 'PRIMARY KEY'},
        'start_datetime': {'type': 'TIMESTAMP'},
        'route_id': {'type': 'VARCHAR(10)'}
    },
    'stop_time_update': {
        'id': {'type': 'SERIAL', 'constraints': 'PRIMARY KEY'},
        'trip_id': {'type': 'VARCHAR(255)'},
        'stop_id': {'type': 'VARCHAR(255)'},
        'arrival_time': {'type': 'TIMESTAMP'},
        'departure_time': {'type': 'TIMESTAMP'}
    },
}

# Schema for alert tables including alert details, active periods, and entities informed
ALERT_TABLES = {
    'alerts': {
        'id': {'type': 'SERIAL', 'constraints': 'PRIMARY KEY'},
        'alert_id': {'type': 'VARCHAR(255)', 'constraints': 'UNIQUE NOT NULL'},
        'header_text': {'type': 'TEXT'},
        'description_text': {'type': 'TEXT'},
        'last_updated': {'type': 'TIMESTAMP', 'constraints': 'DEFAULT NOW()'}
    },
    'active_periods': {
        'id': {'type': 'SERIAL', 'constraints': 'PRIMARY KEY'},
        'alert_id': {'type': 'VARCHAR(255)', 'constraints': 'REFERENCES alerts(alert_id) ON DELETE CASCADE'},
        'start_time': {'type': 'TIMESTAMP'},
        'end_time': {'type': 'TIMESTAMP'}
    },
    'informed_entities': {
        'id': {'type': 'SERIAL', 'constraints': 'PRIMARY KEY'},
        'alert_id': {'type': 'VARCHAR(255)', 'constraints': 'REFERENCES alerts(alert_id) ON DELETE CASCADE'},
        'agency_id': {'type': 'VARCHAR(255)'},
        'route_id': {'type': 'VARCHAR(255)'},
        'stop_id': {'type': 'VARCHAR(255)'}
    },
}

# User and subscription management tables for handling user preferences
USER_TABLES = {
    'users': {
        'user_id': {'type': 'SERIAL', 'constraints': 'PRIMARY KEY'},
        'username': {'type': 'VARCHAR(255)', 'constraints': 'UNIQUE NOT NULL'}
    },
    'subscriptions': {
        'subscription_id': {'type': 'SERIAL', 'constraints': 'PRIMARY KEY'},
        'user_id': {'type': 'INT', 'constraints': 'REFERENCES users(user_id) ON DELETE CASCADE'},
        'stop_id': {'type': 'VARCHAR(255)'},
        'route_id': {'type': 'VARCHAR(255)'},
    },
}

def get_column_type(dtype):
    """
    Map Pandas data types to PostgreSQL-compatible data types.
    
    Args:
        dtype (pd.Dtype): Data type of a DataFrame column.
    
    Returns:
        str: The PostgreSQL data type corresponding to the Pandas data type.
    """
    if pd.api.types.is_integer_dtype(dtype):
        return "INTEGER"
    elif pd.api.types.is_float_dtype(dtype):
        return "FLOAT"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"
    else:
        return "TEXT"

def create_table_from_df(db, df, table_name, primary_key):
    """
    Create a PostgreSQL table based on the structure of a DataFrame.
    
    Args:
        db (Database): An instance of the Database class for interaction with PostgreSQL.
        df (pd.DataFrame): DataFrame whose columns are used to infer the table schema.
        table_name (str): The name of the table to create in PostgreSQL.
        primary_key (str): The column that should act as the primary key, if applicable.
    """
    # Initialize columns dictionary for table schema
    columns = {}
    for col_name, dtype in zip(df.columns, df.dtypes):
        columns[col_name] = {'type': get_column_type(dtype)}

    # Set primary key constraint if a primary key is defined
    if primary_key:
        columns[primary_key]['constraints'] = 'PRIMARY KEY'

    # Create the table in the database
    db.create_table(table_name, columns, primary_key)

    # Add an additional column 'trains' to the 'stops' table if it doesn't already exist
    if table_name == 'stops':
        alter_table_query = """
        ALTER TABLE stops ADD COLUMN IF NOT EXISTS trains TEXT;
        """
        db.execute_sql_query(alter_table_query)

def create_static_tables(db):
    """
    Create tables for static MTA data from CSV files.
    
    Args:
        db (Database): An instance of the Database class for interacting with PostgreSQL.
    """
    for csv_file, table_name in CSV_TO_TABLE_MAP.items():
        csv_path = os.path.join(CSV_DIR, csv_file)
        
        # Check if the CSV file exists before proceeding
        if os.path.exists(csv_path):
            df = pd.read_csv(csv_path)
            primary_key = PRIMARY_KEYS.get(table_name)
            create_table_from_df(db, df, table_name, primary_key)
        else:
            logger.warning(f"CSV file {csv_file} does not exist at path {csv_path}.")

def create_real_time_and_alert_tables(db):
    """
    Create PostgreSQL tables for real-time transit data, alerts, and user subscriptions.
    
    Args:
        db (Database): Instance of the Database class for PostgreSQL interaction.
    """
    
    for table_name, columns in REAL_TIME_TABLES.items():
        db.create_table(table_name, columns)
    
    for table_name, columns in ALERT_TABLES.items():
        db.create_table(table_name, columns)
    
    for table_name, columns in USER_TABLES.items():
        db.create_table(table_name, columns)

if __name__ == "__main__":
    # Initialize the database connection pool
    db = Database()
    db.initialize_pool()
    
    # Create static data tables from CSV files
    create_static_tables(db)
    
    # Create real-time and alert-related tables
    create_real_time_and_alert_tables(db)
    
    # Log success message on completion of schema creation
    logger.info("Schema creation completed successfully.")
