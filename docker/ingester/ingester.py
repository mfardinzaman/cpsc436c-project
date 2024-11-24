import json
import statistics
from datetime import datetime
import urllib.parse
import boto3
from cassandra.cluster import Cluster, DCAwareRoundRobinPolicy
from ssl import SSLContext, PROTOCOL_TLSv1_2 , CERT_REQUIRED
from cassandra_sigv4.auth import SigV4AuthProvider
from cassandra.query import BatchStatement, ConsistencyLevel, BatchType


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
    return session


def create_batch():
    return BatchStatement(batch_type=BatchType.UNLOGGED, consistency_level=ConsistencyLevel.LOCAL_QUORUM)


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
    
    return route_key, stop_time_updates


def get_stop_info(stop):
    try:
        stop_sequence = stop['stopSequence']
        arrival_delay = stop['arrival']['delay']
        arrival_time = stop['arrival']['time']
        departure_delay = stop['departure']['delay']
        stop_id = stop['stopId']
        return stop_id, arrival_delay
    except KeyError:
        return None


def get_most_recent_stop_info(stop_updates):
    for stop in reversed(stop_updates):
        info = get_stop_info(stop)
        if info is not None:
            return info
    return None
        
        
def get_stats(delays):
    stats = {
        'mean': round(statistics.mean(delays)),
        'median': round(statistics.median(delays)),
        'count': len(delays),
        'very_early': sum(delay <= -HIGH_DELAY for delay in delays),
        'very_late': sum(delay >= HIGH_DELAY for delay in delays)
    }
    return stats
        


def read_data(json_string):        
    routes = {}
    stops = {}
    data = json.loads(json_string)
    current_route = None
    for trip_data_string in data:
        if current_route is not None:
            routes[route_key] = current_route
            current_route = None
        
        trip_data = json.loads(trip_data_string)
        try:
            route_key, stop_time_updates = get_trip_info(trip_data)
        except KeyError:
            continue
        
        info = get_most_recent_stop_info(stop_time_updates)
        if info is None:
            continue
        _, delay = info
        try:
            routes[route_key].append(delay)
        except KeyError:
            routes[route_key] = [delay]
        
        # TODO: record stops in database and check if stops were already recorded to prevent duplicates
        # Assemble delays for all stops within the last X minutes
        for stop in stop_time_updates:
            info = get_stop_info(stop)
            if info is None:
                continue
            stop_id, delay = info
            try:
                stops[stop_id].append(delay)
            except:
                stops[stop_id] = [delay]
                    
    route_stats = {}
    for route_key, delays in routes.items():
        stats = get_stats(delays)
        route_stats[route_key] = stats
    stop_stats = {}
    for stop_id, delays in stops.items():
        stats = get_stats(delays)
        stop_stats[stop_id] = stats
    return route_stats, stop_stats


def ingest_route_stats_by_route(session, route_stats, update_time):
    insert_user = session.prepare(
        """
        INSERT INTO route_stat_by_route (
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
    
    batch = create_batch()
    count = 0
    for route_key, stats in route_stats.items():
        if count == 30:
            session.execute(batch)
            batch = create_batch()
            count = 0
        batch.add(insert_user, (
            route_key[0],
            route_key[1],
            stats['mean'],
            stats['median'],
            stats['very_early'],
            stats['very_late'],
            stats['count'],
            update_time
        ))
        count += 1
    session.execute(batch)


def get_string_and_upload_time(event):
    s3_client = boto3.client('s3')
    s3_Bucket_Name = event["Records"][0]["s3"]["bucket"]["name"]
    s3_File_Name = urllib.parse.unquote(event["Records"][0]["s3"]["object"]["key"].replace("+", " "))
    print("Accessing file", s3_File_Name, "in bucket", s3_Bucket_Name)
    upload_time = datetime.fromisoformat(event["Records"][0]["eventTime"])
    object = s3_client.get_object(Bucket=s3_Bucket_Name, Key=s3_File_Name)
    body = object['Body']
    json_string = body.read().decode('utf-8')
    return json_string, upload_time


def lambda_handler(event, context):
    try:
        json_string, upload_time = get_string_and_upload_time(event)
        route_stats, stop_stats = read_data(json_string)

        session = create_session()
        ingest_route_stats_by_route(session, route_stats, upload_time)

        return {
            'statusCode': 200,
            'body': 'Success'
        }
    except Exception as err:
        print(err)

    return {
        'statusCode': 400,
        'body': 'Failure'
    }
