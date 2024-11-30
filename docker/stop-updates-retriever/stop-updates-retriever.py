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
    
def get_stop_updates(session, update_time, stop_id):
    # query = create_statement(f"SELECT * FROM stop_update WHERE update_time > '{datetime.date(datetime.now()) - timedelta(hours=1)}' AND stop_id = '{stop_id}';")
    # results = session.execute(query)

    prepared = session.prepare("SELECT * FROM stop_update WHERE update_time = ? AND stop_id = ?")
    bound = prepared.bind((update_time, stop_id))
    results = session.execute(bound)
    return results

def get_routes(session):
    query = create_statement(f"SELECT route_id, direction_id, route_short_name, direction_name FROM route;")
    results = session.execute(query)
    return results

def lambda_handler(event, context):
    try:
        session = create_session()
        update_time = get_last_update_time(session)
        stops = get_stop_updates(session, update_time, event['stop_id'])

        routes = get_routes(session)
        
        map = {}

        for route in routes:
            map[(route.route_id, route.direction_id)] = (route.route_short_name, route.direction_name)

        results = []
        for stopData in stops:
            result = {
                'trip_id': stopData.trip_id,
                'stop_id': stopData.stop_id,
                'update_time': stopData.update_time.isoformat(),
                'delay': stopData.delay,
                'direction_id': stopData.direction_id,
                'route_id': stopData.route_id,
                'stop_time': stopData.stop_time.isoformat(),
                'vehicle_label': stopData.vehicle_label,
                'route_short_name': map[(stopData.route_id, stopData.direction_id)][0],
                'direction_name': map[(stopData.route_id, stopData.direction_id)][1],
            }
            # stopData.update_time = str stopData.update_time.isoformat())
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
