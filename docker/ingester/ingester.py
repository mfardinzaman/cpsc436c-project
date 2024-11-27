import json
import statistics
from datetime import datetime, timezone
import urllib.parse
import boto3
from cassandra.cluster import Cluster, DCAwareRoundRobinPolicy
from ssl import SSLContext, PROTOCOL_TLSv1_2 , CERT_REQUIRED
from cassandra_sigv4.auth import SigV4AuthProvider
from cassandra.query import SimpleStatement, BatchStatement, ConsistencyLevel, BatchType
from cassandra.concurrent import execute_concurrent, execute_concurrent_with_args 


# Number of seconds deviance for a bus to be considered "very late" or "very early"
HIGH_DELAY = 300


def create_session():
    ssl_context = SSLContext(PROTOCOL_TLSv1_2)
    ssl_context.load_verify_locations('./sf-class2-root.crt')
    ssl_context.verify_mode = CERT_REQUIRED

    auth_provider = SigV4AuthProvider(boto3.DEFAULT_SESSION)

    cluster = Cluster(
        ['cassandra.ca-central-1.amazonaws.com'], 
        ssl_context=ssl_context, 
        auth_provider=auth_provider, 
        port=9142,
        load_balancing_policy=DCAwareRoundRobinPolicy(),
        protocol_version=4
    )
    session = cluster.connect(keyspace='Translink')
    session.default_timeout = 60
    session.default_consistency_level = ConsistencyLevel.LOCAL_QUORUM
    return session


def create_statement(query):
    return SimpleStatement(query_string=query, consistency_level=ConsistencyLevel.LOCAL_QUORUM)



def get_trip_info(trip_data):
    trip_id = trip_data['id']
    trip_update = trip_data['tripUpdate']
    trip = trip_update['trip']
    trip_date = trip['startDate']
    schedule_relationship = trip['scheduleRelationship']
    route_id = trip['routeId']
    direction_id = trip['directionId']
    vehicle = trip_update['vehicle']['label']
    
    route_key = (route_id, direction_id)
    stop_time_updates = trip_update['stopTimeUpdate']
    
    return route_key, stop_time_updates, trip_id, vehicle


def get_stop_info(stop):
    try:
        stop_sequence = stop['stopSequence']
        arrival_delay = stop['arrival']['delay']
        arrival_time = datetime.fromtimestamp(int(stop['arrival']['time']), tz=timezone.utc)
        arrival_time -= arrival_time.utcoffset()
        departure_delay = stop['departure']['delay']
        stop_id = stop['stopId']
        return stop_id, arrival_delay, arrival_time
    except KeyError:
        return None


def get_next_stop_info(stop_updates):
    if len(stop_updates) == 0:
        return None
    return get_stop_info(stop_updates[0])
        
        
def get_stats(delays):
    stats = {
        'mean': round(statistics.mean(delays)),
        'median': round(statistics.median(delays)),
        'count': len(delays),
        'very_early': sum(delay <= -HIGH_DELAY for delay in delays),
        'very_late': sum(delay >= HIGH_DELAY for delay in delays)
    }
    return stats


def get_route_stats(route_data):
    route_stats = {}
    for route_key, delays in route_data.items():
        stats = get_stats(delays)
        route_stats[route_key] = stats
    return route_stats


def get_stop_stats(stop_data):
    stop_stats = {}
    for stop, delays in stop_data.items():
        stats = get_stats(delays)
        stop_stats[stop] = stats
    return stop_stats


def read_data(json_string, update_time):        
    routes = {}
    stops = {}
    stop_params = []
    
    data = json.loads(json_string)
    stop_count = 0
    trip_count = 0
    current_route = None
    for trip_data_string in data:
        if current_route is not None:
            routes[route_key] = current_route
            current_route = None
        
        trip_data = json.loads(trip_data_string)
        try:
            route_key, stop_time_updates, trip_id, vehicle = get_trip_info(trip_data)
        except KeyError:
            continue
        
        info = get_next_stop_info(stop_time_updates)
        if info is None:
            continue
        _, delay, _ = info
        try:
            routes[route_key].append(delay)
        except KeyError:
            routes[route_key] = [delay]
        trip_count += 1
        
        for stop in stop_time_updates:
            info = get_stop_info(stop)
            if info is None:
                continue
            stop_id, delay, arrival = info
            try:
                stops[stop_id].append(delay)
            except KeyError:
                stops[stop_id] = [delay]
            stop_params.append((
                stop_id,
                trip_id,
                route_key[0],
                route_key[1],
                vehicle,
                delay,
                arrival,
                update_time
            ))
            stop_count += 1
    print(f"Read updates for {trip_count} trips on {len(routes)} routes.")
    print(f"Read updates for {stop_count} stop events at {len(stops)} stops.")
    return routes, stops, stop_params


def get_route_data(session, route_stats):
    select_statement = session.prepare("SELECT * FROM route WHERE route_id = ? AND direction_id = ?")
    results = execute_concurrent_with_args(session, select_statement, [route_key for route_key in route_stats.keys()])
    
    route_data = {}
    for (success, result) in results:
        if not success:
            print("ERROR:", result)
        else:
            result = result.all()
            if len(result) > 0:
                row = result[0]
                route_data[(row.route_id, row.direction_id)] = row
    return route_data


def get_stop_data(session, stop_stats):
    select_statement = session.prepare("SELECT * FROM stop WHERE stop_id = ?")
    results = execute_concurrent_with_args(session, select_statement, [(stop_id,) for stop_id in stop_stats.keys()])
    stop_data = {}
    for (success, result) in results:
        if not success:
            print("ERROR:", result)
        else:
            result = result.all()
            if len(result) > 0:
                row = result[0]
                stop_data[row.stop_id] = row
    return stop_data


def ingest_route_stats_by_route(session, route_stats, update_time):    
    print(f"Ingesting {len(route_stats)} records to route_stat_by_route")
    insert_stat = session.prepare(
        """
        INSERT INTO route_stat_by_route(
            route_id, 
            direction_id,
            average_delay, 
            median_delay, 
            very_early_count,
            very_late_count,
            vehicle_count,
            update_time
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
    )
    statements_and_params = []
    for route_key, stats in route_stats.items():
        params = (
            route_key[0], 
            route_key[1],
            stats['mean'],
            stats['median'],
            stats['very_early'],
            stats['very_late'],
            stats['count'],
            update_time
        )
        statements_and_params.append((insert_stat, params))
    
    results = execute_concurrent(session, statements_and_params, raise_on_first_error=False)
    for (success, result) in results:
        if not success:
            print("ERROR:", result)


def ingest_route_stats_by_time(session, route_stats, route_results, update_time):
    print(f"Ingesting {len(route_stats)} records to route_stat_by_time")
    insert_stat = session.prepare(
        """
        INSERT INTO route_stat_by_time (
            route_id,
            route_short_name, 
            route_long_name, 
            route_type, 
            direction_id, 
            direction, 
            direction_name, 
            average_delay, 
            median_delay, 
            very_early_count,
            very_late_count,
            vehicle_count,
            day,
            update_time
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    )
    statements_and_params = []
    for route_key, stats, in route_stats.items():
        route_details = route_results[route_key]
        params = (
            route_key[0],
            route_details.route_short_name,
            route_details.route_long_name,
            route_details.route_type,
            route_key[1],
            route_details.direction,
            route_details.direction_name,
            stats['mean'],
            stats['median'],
            stats['very_early'],
            stats['very_late'],
            stats['count'],
            update_time.date(),
            update_time
        )
        statements_and_params.append((insert_stat, params))
        
    results = execute_concurrent(session, statements_and_params, raise_on_first_error=False, concurrency=50)
    for (success, result) in results:
        if not success:
            print("ERROR:", result)



def ingest_stop_stats_by_stop(session, stop_stats, update_time):
    print(f"Ingesting {len(stop_stats)} records to stop_stat_by_stop")
    insert_stat = session.prepare(
        """
        INSERT INTO stop_stat_by_stop (
            stop_id, 
            average_delay, 
            median_delay, 
            very_early_count,
            very_late_count,
            stop_count,
            update_time
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """
    )
    statements_and_params = []
    for stop_id, stats in stop_stats.items():
        params = (
            stop_id,
            stats['mean'],
            stats['median'],
            stats['very_early'],
            stats['very_late'],
            stats['count'],
            update_time
        )
        statements_and_params.append((insert_stat, params))
    
    results = execute_concurrent(session, statements_and_params, raise_on_first_error=False)
    for (success, result) in results:
        if not success:
            print("ERROR:", result)


def ingest_stop_stats_by_time(session, stop_stats, stop_results, update_time):
    print(f"Ingesting {len(stop_stats)} records to stop_stat_by_time")
    insert_stat = session.prepare(
        """
        INSERT INTO stop_stat_by_time(
            stop_id, 
            stop_code,
            stop_name,
            latitude,
            longitude,
            zone_id,
            location_type,
            wheelchair_boarding,
            average_delay, 
            median_delay, 
            very_early_count,
            very_late_count,
            stop_count,
            day,
            update_time
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    )
    
    statements_and_params = []
    for stop_id, stats in stop_stats.items():
        try:
            stop_details = stop_results[stop_id]
        except KeyError:
            continue
        params = (
            stop_id,
            stop_details.stop_code,
            stop_details.stop_name,
            stop_details.latitude,
            stop_details.longitude,
            stop_details.zone_id,
            stop_details.location_type,
            stop_details.wheelchair_boarding,
            stats['mean'],
            stats['median'],
            stats['very_early'],
            stats['very_late'],
            stats['count'],
            update_time.date(),
            update_time
        )
        statements_and_params.append((insert_stat, params))
    
    results = execute_concurrent(session, statements_and_params, raise_on_first_error=False, concurrency=50)
    for (success, result) in results:
        if not success:
            print("ERROR:", result)
            

def ingest_stop_updates(session, stop_params):
    print(f"Ingesting {len(stop_params)} records to stop_update")
    insert_stat = session.prepare(
        """
        INSERT INTO stop_update(stop_id, trip_id, route_id, direction_id, vehicle_label, delay, stop_time, update_time)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
    )
    results = execute_concurrent_with_args(session, insert_stat, stop_params, concurrency=50)
    for (success, result) in results:
        if not success:
            print("ERROR: ", result)


def get_string_and_upload_time(event):
    s3_client = boto3.client('s3')
    s3_Bucket_Name = event["Records"][0]["s3"]["bucket"]["name"]
    s3_File_Name = urllib.parse.unquote(event["Records"][0]["s3"]["object"]["key"].replace("+", " "))
    print("Accessing file", s3_File_Name, "in bucket", s3_Bucket_Name)
    upload_time = datetime.fromisoformat(event["Records"][0]["eventTime"]).replace(tzinfo=timezone.utc)
    object = s3_client.get_object(Bucket=s3_Bucket_Name, Key=s3_File_Name)
    body = object['Body']
    json_string = body.read().decode('utf-8')
    return json_string, upload_time


def lambda_handler(event, context):
    try:
        json_string, upload_time = get_string_and_upload_time(event)

        session = create_session()
        
        # Get route and stop updates from update file
        print("Reading trip updates from update file...")
        routes, stops, stop_params = read_data(json_string, upload_time)
        
        # Interpret route and stop updates into statistics
        print("Generating statistics...")
        route_stats = get_route_stats(routes)
        stop_stats = get_stop_stats(stops)
        
        # Get route details for routes with statistics
        print("Getting details for routes...")
        route_detail_results = get_route_data(session, route_stats)
        print("Getting details for stops...")
        stop_detail_results = get_stop_data(session, stop_stats)
        
        # Ingest statistics
        print("Beginning ingestion...")
        ingest_route_stats_by_route(session, route_stats, upload_time)
        ingest_route_stats_by_time(session, route_stats, route_detail_results, upload_time)
        ingest_stop_stats_by_stop(session, stop_stats, upload_time)
        ingest_stop_stats_by_time(session, stop_stats, stop_detail_results, upload_time)
        ingest_stop_updates(session, stop_params)
        
        print("Ingestion complete!")

        return {
            'statusCode': 200,
            'body': 'Success'
        }
    except Exception as err:
        print("ERROR:", err)

    return {
        'statusCode': 400,
        'body': 'Failure'
    }
