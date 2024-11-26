import json
import statistics
from datetime import datetime, timezone
import urllib.parse
import boto3
from cassandra.cluster import Cluster, DCAwareRoundRobinPolicy
from ssl import SSLContext, PROTOCOL_TLSv1_2 , CERT_REQUIRED
from cassandra_sigv4.auth import SigV4AuthProvider
from cassandra.query import SimpleStatement, BatchStatement, ConsistencyLevel, BatchType


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
    return session


def create_batch():
    return BatchStatement(batch_type=BatchType.UNLOGGED, consistency_level=ConsistencyLevel.LOCAL_QUORUM)


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


def read_data(session, json_string):        
    routes = {}
    stops = {}
    stop_updates = set()
    results = []
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
            statement = create_statement(
                f"""
                INSERT INTO stop_update(stop_id, trip_id, route_id, direction_id, vehicle_label, delay, stop_time)
                VALUES ('{stop_id}', '{trip_id}', '{route_key[0]}', {route_key[1]}, '{vehicle}', {delay}, '{arrival.isoformat(timespec='milliseconds')}')
                """
            )
            results.append(session.execute_async(statement))
            stop_count += 1
        if len(results) > 1000:
            block_for_results(results)
            results = []
    block_for_results(results)
    print(f"Read updates for {trip_count} trips on {len(routes)} routes.")
    print(f"Read updates for {stop_count} stop events at {len(stops)} stops.")
    return routes, stops


def get_route_data(session, route_stats):
    results = {}
    for route_id, direction_id in route_stats.keys():
        query = create_statement(f"SELECT * FROM route WHERE route_id = '{route_id}' AND direction_id = {direction_id};")
        results[(route_id, direction_id)] = session.execute_async(query)
    return results


def get_stop_data(session, stop_stats):
    results = {}
    for stop_id in stop_stats.keys():
        query = create_statement(f"SELECT * FROM stop WHERE stop_id = '{stop_id}';")
        results[stop_id] = session.execute_async(query)
    return results


def ingest_route_stats_by_route(session, route_stats, update_time):    
    results = []
    print(f"Ingesting {len(route_stats)} records to route_stats_by_route")
    for route_key, stats in route_stats.items():
        statement = create_statement(
            f"""
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
            VALUES (
                '{route_key[0]}', 
                {route_key[1]}, 
                {stats['mean']},
                {stats['median']},
                {stats['very_early']},
                {stats['very_late']},
                {stats['count']},
                '{update_time.isoformat(timespec='milliseconds')}'
            )
            """
        )
        results.append(session.execute_async(statement))
    block_for_results(results)


def ingest_route_stats_by_time(session, route_stats, route_results, update_time):
    results = []
    
    print(f"Ingesting {len(route_stats)} records to route_stats_by_time")
    prepared_string = """
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
    insert_stat = session.prepare(prepared_string)
    
    batch = create_batch()
    count = 0
    for route_key, stats, in route_stats.items():
        if count == 30:
            results.append(session.execute_async(batch))
            insert_stat = session.prepare(prepared_string)
            batch = create_batch()
            count = 0
        route_details = route_results[route_key].result()[0]
        batch.add(insert_stat, (
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
        ))
        count += 1
    results.append(session.execute_async(batch))
    block_for_results(results)



def ingest_stop_stats_by_stop(session, stop_stats, update_time):
    results = []
    print(f"Ingesting {len(stop_stats)} records to stop_stats_by_stop")
    for stop_id, stats in stop_stats.items():
        statement = create_statement(
            f"""
            INSERT INTO stop_stat_by_stop (
                stop_id, 
                average_delay, 
                median_delay, 
                very_early_count,
                very_late_count,
                stop_count,
                update_time
            )
            VALUES (
                '{stop_id}', 
                {stats['mean']},
                {stats['median']},
                {stats['very_early']},
                {stats['very_late']},
                {stats['count']},
                '{update_time.isoformat(timespec='milliseconds')}'
            )
            """
        )
        results.append(session.execute_async(statement))
        if len(results) > 50:
            block_for_results(results)
            results = []
    block_for_results(results)


def ingest_stop_stats_by_time(session, stop_stats, stop_results, update_time):
    results = []
    
    print(f"Ingesting {len(stop_stats)} records to stop_stats_by_time")
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
    
    batch = create_batch()
    count = 0
    for stop_id, stats, in stop_stats.items():
        if count == 30:
            session.execute(batch)
            batch = create_batch()
            count = 0
        all_stop_details = stop_results[stop_id].result().all()
        if (len(all_stop_details) == 0):
            continue
        stop_details = all_stop_details[0]
        batch.add(insert_stat, (
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
        ))
        count += 1
        if len(results) > 10:
            block_for_results(results)
            results = []
    results.append(session.execute_async(batch))
    block_for_results(results)


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


def block_for_results(results):
    for result in results:
        result.result()


def lambda_handler(event, context):
    try:
        json_string, upload_time = get_string_and_upload_time(event)

        session = create_session()
        
        # Get route and stop updates from update file
        # Incidentally adds stop updates to the database
        print("Reading trip updates from update file...")
        routes, stops = read_data(session, json_string)
        
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
        
        print("Ingestion complete!")

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
