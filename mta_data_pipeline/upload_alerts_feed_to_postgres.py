import logging
from utils.helpers import fetch_gtfs_realtime_data, process_gtfs_feed_data
from utils.db import Database

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Define the MTA GTFS real-time API endpoint
url = 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/camsys%2Fsubway-alerts'

if __name__ == "__main__":
    # Initialize the database connection pool
    db = Database()
    db.initialize_pool()

    try:
        # Fetch GTFS real-time data from the MTA API
        feed = fetch_gtfs_realtime_data(url)
        
        # Process the GTFS feed to extract relevant alert information
        _, _, alerts = process_gtfs_feed_data([feed])

        for alert in alerts:
            alert_id = alert['alert_id']
            set_columns = {
                'header_text': alert['header_text'],
                'description_text': alert['description_text'],
            }
            where_conditions = {'alert_id': alert_id}

            # Check if the alert already exists in the database
            existing_alert = db.execute_sql_query("SELECT alert_id FROM alerts WHERE alert_id = %s", (alert_id,))
            
            if existing_alert:
                # Update the existing alert record with new data
                db.update_records('alerts', set_columns, where_conditions, 
                                  {**set_columns, **where_conditions})
            else:
                # Insert a new alert record into the database
                db.insert_data('alerts', ['alert_id', 'header_text', 'description_text'],
                               [(alert_id, set_columns['header_text'], set_columns['description_text'])])
            
            # Prepare and insert or update active periods associated with the alert
            active_period_rows = [(alert_id, start_time, end_time) for start_time, end_time in alert['active_periods']]
            db.delete_records('active_periods', {'alert_id': alert_id})
            if active_period_rows:
                db.insert_data('active_periods', ['alert_id', 'start_time', 'end_time'], active_period_rows)

            # Prepare and insert or update informed entities associated with the alert
            informed_entity_rows = [(alert_id, agency_id, route_id, stop_id) for agency_id, route_id, stop_id in alert['informed_entities']]
            db.delete_records('informed_entities', {'alert_id': alert_id})
            if informed_entity_rows:
                db.insert_data('informed_entities', ['alert_id', 'agency_id', 'route_id', 'stop_id'], informed_entity_rows)

    except Exception as e:
      logger.error(f"Error updating alerts in the database: {e}")
    
    finally:
        db.close_all_connections()
