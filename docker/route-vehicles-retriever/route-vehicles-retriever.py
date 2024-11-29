import json
import statistics
from datetime import datetime, timedelta
import urllib.parse
import boto3
from cassandra.cluster import Cluster, DCAwareRoundRobinPolicy
from ssl import SSLContext, PROTOCOL_TLSv1_2 , CERT_REQUIRED
from cassandra_sigv4.auth import SigV4AuthProvider
from cassandra.query import SimpleStatement, BatchStatement, ConsistencyLevel, BatchType
from cassandra.concurrent import execute_concurrent, execute_concurrent_with_args


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

def get_last_update_time(session):
    statement = session.prepare("SELECT * FROM update_time WHERE day = ? LIMIT 1")
    now = datetime.now()
    today = now.date()
    yesterday = (now - timedelta(days=1)).date()
    results = execute_concurrent_with_args(session, statement, [(today,), (yesterday,)])
    update_time = None
    for (success, result) in results:
        if not success:
            print("ERROR:", result)
        else:
            result = result.one()
            if update_time is None or result.day == today:
                update_time = result.update_time
    return update_time

def get_vehicle_updates(session, route_id, direction_id, update_time):
    prepared = session.prepare("SELECT * FROM vehicle_by_route WHERE update_time = ? AND route_id = ? AND direction_id = ?")
    bound = prepared.bind((update_time, route_id, direction_id))
    results = session.execute(bound)
    return results

    
# def get_stop_updates(session, route_id, direction_id):
#     query = create_statement(f"SELECT * FROM vehicle_by_route WHERE update_time > '{datetime.date(datetime.now()) - timedelta(hours=1)}' AND route_id = '{route_id}' AND direction_id = {direction_id};")
#     results = session.execute(query)

#     return results

def get_stops(session):
    query = create_statement(f"SELECT stop_id, stop_name FROM stop;")
    results = session.execute(query)
    return results

def lambda_handler(event, context):
    try:
        session = create_session()
        update_time = get_last_update_time(session)
        vehicles = get_vehicle_updates(session, event['route_id'], event['direction_id'], update_time)

        stopIdNames = get_stops(session)
        
        id_name_map = {}

        for stop in stopIdNames:
            id_name_map[stop.stop_id] = stop.stop_name


        results = []
        latestSet = False
        for vehicle in vehicles:
            result = {
                'route_id': vehicle.route_id,
                'direction_id': vehicle.direction_id,
                'update_time': vehicle.update_time.isoformat(),
                'delay': vehicle.delay,
                'stop_sequence': vehicle.stop_sequence,
                'vehicle_id': vehicle.vehicle_id,
                'expected_arrival': vehicle.expected_arrival.isoformat(),
                'vehicle_label': vehicle.vehicle_label,
                'stop_id': vehicle.stop_id,
                'trip_id': vehicle.trip_id,
                'stop_name': id_name_map[vehicle.stop_id]
            }
            # vehicle.update_time = str vehicle.update_time.isoformat())
            results.append(result)

        return {
            'statusCode': 200,
            'body': results
        }
    except Exception as err:
        print(err)

    return {
        'statusCode': 400,
        'body': 'Failure'
    }
