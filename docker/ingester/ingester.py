import json
import statistics
import boto3
from cassandra.cluster import Cluster
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
        port=9142
    )
    session = cluster.connect(keyspace='Translink')
    return session


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


def get_string_from_object(event):
    s3_client = boto3.client('s3')
    s3_Bucket_Name = event["Records"][0]["s3"]["bucket"]["name"]
    s3_File_Name = event["Records"][0]["s3"]["object"]["key"]
    object = s3_client.get_object(Bucket=s3_Bucket_Name, Key=s3_File_Name)
    body = object['Body']
    json_string = body.read().decode('utf-8')
    return json_string


def lambda_handler(event, context):
    try:
        json_string = get_string_from_object(event)
        route_stats, stop_stats = read_data(json_string)

        session = create_session()

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
