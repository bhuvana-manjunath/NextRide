from utils.db import Database

# Initialize the Database instance
db = Database()
db.initialize_pool()

def get_station_options():
    """
    Fetches a list of distinct stations from the database, excluding 
    stops with IDs that end in 'N' or 'S'. The function returns details 
    including stop ID, stop name, address, and the trains that serve the station.

    Returns:
        list: A list of tuples with the station's stop ID, stop name, address, and trains.
    """
    
    # SQL query to retrieve distinct station details
    query = """SELECT DISTINCT ON (stop_name, address) stop_id, stop_name, address, trains
               FROM public.stops
               WHERE stop_id NOT LIKE '%N' AND stop_id NOT LIKE '%S'
               ORDER BY stop_name, address, stop_id;"""

    # Execute the query
    results = db.execute_sql_query(query)
    
    # Return a list of station details as tuples
    return [
        (row['stop_id'], row['stop_name'], row['address'], row['trains'])
        for row in results
    ]

def get_route_options():
    """
    Fetches the list of available routes from the database, 
    including route ID, short name, and long name. 
    Routes are ordered by their ID.

    Returns:
        list: A list of tuples containing route ID, short name, and long name.
    """
    
    # SQL query to retrieve route details
    query = """SELECT route_id, route_short_name, route_long_name 
               FROM public.routes
               ORDER BY route_id ASC"""
    
    # Execute the query
    results = db.execute_sql_query(query)
    
    # Return a list of route details as tuples
    return [
        (row['route_id'], row['route_short_name'], row['route_long_name'])
        for row in results
    ]

def get_trains(stop_id):
    """
    Fetches the train lines that serve a specific stop based on the stop ID.

    Args:
        stop_id (str): The stop ID for which the train lines need to be fetched.
    
    Returns:
        list: A list of train lines (as strings) serving the stop.
    """
    
    # SQL query to fetch train lines for the given stop
    query = """SELECT trains FROM public.stops WHERE stop_id = %s;"""
    
    # Execute the query
    results = db.execute_sql_query(query, (stop_id,))
    
    # Return the list of trains serving the stop
    return [row['trains'] for row in results]

def get_station_departures(station_id):
    """
    Retrieves the next departure times for all routes at a given station, 
    along with the estimated time of arrival (ETA) in minutes.

    Args:
        station_id (str): The ID of the station to retrieve departure times for.
    
    Returns:
        list: A list of tuples containing the route ID, departure time, and ETA in minutes.
    """
    
    # SQL query to fetch next departures for all routes at the given station
    query = """
    SELECT DISTINCT ON (t.route_id)  
           t.route_id,  
           stu.departure_time, 
           ROUND(EXTRACT(EPOCH FROM (stu.departure_time - NOW())) / 60) AS ETA
    FROM public.trips_real_time AS t
    INNER JOIN public.stop_time_update AS stu ON t.trip_id = stu.trip_id
    INNER JOIN public.stops AS s ON stu.stop_id = s.stop_id
    WHERE s.stop_id = %s AND stu.departure_time > NOW()
    ORDER BY t.route_id, stu.departure_time ASC;
    """
    
    # Execute the query
    results = db.execute_sql_query(query, (station_id,))
    
    # Return a list of route IDs, departure times, and ETAs
    return [
        (row['route_id'], row['departure_time'], row['eta'])
        for row in results
    ]

def get_route_departures(station_id, route_name):
    """
    Retrieves the next three departures for a specific route at the given station, 
    along with the estimated time of arrival (ETA) in minutes.

    Args:
        station_id (str): The station's ID.
        route_name (str): The route's short name (e.g., 'A', 'B', '1').
    
    Returns:
        list: A list of tuples containing route ID, departure time, and ETA in minutes.
    """
    
    # SQL query to fetch the next three departures for a specific route at the station
    query = """
    SELECT t.route_id, stu.departure_time, 
           ROUND(EXTRACT(EPOCH FROM (stu.departure_time - NOW())) / 60) AS ETA
    FROM public.trips_real_time AS t
    INNER JOIN public.stop_time_update AS stu ON t.trip_id = stu.trip_id
    INNER JOIN public.stops AS s ON stu.stop_id = s.stop_id
    WHERE s.stop_id = %s AND t.route_id = %s AND stu.departure_time > NOW()
    ORDER BY stu.departure_time ASC
    LIMIT 3;
    """
    
    # Execute the query
    results = db.execute_sql_query(query, (station_id, route_name))
    
    # Return a list of route IDs, departure times, and ETAs
    return [
        (row['route_id'], row['departure_time'], row['eta'])
        for row in results
    ]

def fetch_user_alerts(user_id):
    """
    Retrieves active alerts for a user based on their subscriptions. 
    Alerts are categorized as 'upcoming', 'active', or 'past' based on time.

    Args:
        user_id (str): The user's ID for which alerts are fetched.
    
    Returns:
        list: A list of tuples containing alert ID, header, description, time period, 
              alert status, and the related entity (route/stop).
    """
    
    # SQL query to retrieve active alerts for the user
    query = """
    SELECT a.alert_id, a.header_text, a.description_text, ap.start_time, ap.end_time,
           CASE
               WHEN ap.start_time > NOW() THEN 'upcoming'
               WHEN ap.end_time < NOW() THEN 'past'
               ELSE 'active'
           END AS alert_status,
           COALESCE(ie.route_id, ie.stop_id) AS entity_id
    FROM alerts a
    JOIN active_periods ap ON a.alert_id = ap.alert_id
    JOIN informed_entities ie ON a.alert_id = ie.alert_id
    JOIN subscriptions s ON (s.route_id = ie.route_id OR s.stop_id = ie.stop_id)
    WHERE s.user_id = %s
    ORDER BY entity_id, alert_status DESC, ap.start_time ASC;
    """
    
    # Execute the query
    results = db.execute_sql_query(query, (user_id,))
    
    # Return a list of alert details and statuses
    return [
        (row['alert_id'], row['header_text'], row['description_text'], row['start_time'], row['end_time'], row['alert_status'], row['entity_id'])
        for row in results
    ]

def get_user_subscriptions(user_id):
    """
    Fetches the subscriptions of a user, including the stops and routes they are subscribed to.

    Args:
        user_id (str): The user's ID for which subscriptions are fetched.
    
    Returns:
        list: A list of tuples containing subscription ID, stop ID, and route ID.
    """
    
    # SQL query to retrieve the user's subscriptions
    query = """
    SELECT subscription_id, stop_id, route_id 
    FROM subscriptions 
    WHERE user_id = %s
    ORDER BY stop_id, route_id;
    """
    
    # Execute the query
    subscriptions = db.execute_sql_query(query, (user_id,))
    
    # Return a list of subscription details
    return [(row["subscription_id"], row['stop_id'], row['route_id']) for row in subscriptions]
