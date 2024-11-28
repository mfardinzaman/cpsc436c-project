import json
import statistics
from datetime import datetime, timedelta
import urllib.parse
import boto3
from cassandra.cluster import Cluster, DCAwareRoundRobinPolicy
from ssl import SSLContext, PROTOCOL_TLSv1_2 , CERT_REQUIRED
from cassandra_sigv4.auth import SigV4AuthProvider
from cassandra.query import SimpleStatement, BatchStatement, ConsistencyLevel, BatchType


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

    
def get_stop_updates(session, route_id, direction_id):
    query = create_statement(f"SELECT * FROM vehicle_by_route WHERE update_time > '{datetime.date(datetime.now()) - timedelta(hours=1)}' AND route_id = '{route_id}' AND direction_id = {direction_id};")
    results = session.execute(query)

    return results



def lambda_handler(event, context):
    try:
        session = create_session()
        vehicles = get_stop_updates(session, event['route_id'], event['direction_id'])

        results = []
        latestSet = False
        for vehicle in vehicles:
            if (latestSet == False):
                latestUpdate = vehicle.update_time
                latestSet = True
            elif (vehicle.update_time < latestUpdate):
                break
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
                'trip_id': vehicle.trip_id
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
