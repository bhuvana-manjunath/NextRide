import logging
from utils.helpers import fetch_gtfs_realtime_data, process_gtfs_feed_data
from utils.db import Database

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# MTA API URLs
urls = {
    'NQRW': 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-nqrw',
    'ACE': 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace',
    'G': 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-g',
    'one_to_seven': 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs',
    'BDFM': 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-bdfm',
    'JZ': 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-jz',
    'L': 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-l',
    'SIR': 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-si'
}
    

if __name__ == "__main__":
    # Initialize database connection pool
    db = Database()
    db.initialize_pool()

    # Collect and process GTFS feed data
    feed_list = []
    for key, url in urls.items():
        feed = fetch_gtfs_realtime_data(url)
        if feed:
            feed_list.append(feed)
        else:
            logger.warning(f"Failed to fetch data for {key} lines.")
    
    # Process the feed to extract trip and stop time updates
    trip_updates, stop_time_updates, _ = process_gtfs_feed_data(feed_list)

    # Clear existing real-time data before inserting new data
    db.delete_records("trips_real_time")
    db.delete_records("stop_time_update")

    # Insert new trip updates
    if trip_updates:
        db.insert_data(
            table_name="trips_real_time",
            columns=["trip_id", "start_datetime", "route_id"],
            values=trip_updates,
            on_conflict="DO NOTHING"
        )
    else:
        logger.warning("No trip updates available to insert.")

    # Insert new stop time updates
    if stop_time_updates:
        db.insert_data(
            table_name="stop_time_update",
            columns=["trip_id", "stop_id", "arrival_time", "departure_time"],
            values=stop_time_updates,
            on_conflict="DO NOTHING"
        )
    else:
        logger.warning("No stop time updates available to insert.")
    
    db.close_all_connections()
