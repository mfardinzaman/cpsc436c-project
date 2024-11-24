import json
import statistics
import os
from datetime import datetime
from cassandra.cluster import Cluster
from ssl import SSLContext, PROTOCOL_TLSv1_2 , CERT_REQUIRED
import boto3
from cassandra_sigv4.auth import SigV4AuthProvider
from cassandra.query import BatchStatement, ConsistencyLevel, BatchType


HIGH_DELAY = 300


def create_session(access_key_id, secret_access_key, session_token):
    ssl_context = SSLContext(PROTOCOL_TLSv1_2)
    ssl_context.load_verify_locations('../data/sf-class2-root.crt')
    ssl_context.verify_mode = CERT_REQUIRED

    boto_session = boto3.Session(aws_access_key_id=access_key_id,
                                aws_secret_access_key=secret_access_key,
                                aws_session_token=session_token,
                                region_name='ca-central-1')
    auth_provider = SigV4AuthProvider(boto_session)

    cluster = Cluster(['cassandra.ca-central-1.amazonaws.com'], ssl_context=ssl_context, auth_provider=auth_provider,
                    port=9142)
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
        


def read_data(path):        
    routes = {}
    stops = {}
    with open(path, 'r') as f:
        data = json.load(f)
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


def get_recent_stop_updates(session):
    # TODO: Query stop_updates table for recent stops
    return


def ingest_new_stop_updates(session, stop_updates):
    # TODO: Insert stop_updates into its table
    return


def ingest_route_stats_by_route(session, route_stats, update_time):
    insert_user = session.prepare(
        """
        INSERT INTO route_statistic_by_route (
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


def ingest_route_stats_by_time(session, route_stats):
    insert_user = session.prepare(
        """
        INSERT INTO route_statistic_by_time (
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
            update_time
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    )


def ingest_route_stats(session, route_stats):
    # TODO: Insert route_stats into its tables in batches of 30

    return
    


if __name__ == '__main__':
    session = create_session(os.getenv('AWS_ACCESS_KEY_ID'), os.getenv('AWS_SECRET_ACCESS_KEY'), os.getenv('AWS_SESSION_TOKEN'))
    path = '../data/2024-11-21 18_41_40.734247.json'
    route_stats, stop_stats = read_data(path)
    # ingest_route_stats(session, route_stats)
    ingest_route_stats_by_route(session, route_stats)