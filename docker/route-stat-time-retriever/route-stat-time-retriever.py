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

    
def get_route_stats(session, route_id, direction_id):
    query = create_statement(f"SELECT * FROM route_stat_by_route WHERE route_id = '{route_id}' AND direction_id = {direction_id};")
    results = session.execute(query)

    return results



def lambda_handler(event, context):
    try:
        session = create_session()
        data = get_route_stats(session, event['route_id'], event['direction_id'])

        results = []

        for routeData in data:
            result = {
                'direction_id': routeData.direction_id,
                'route_id': routeData.route_id,
                'update_time': routeData.update_time.isoformat(),
                'average_delay': routeData.average_delay,
                'median_delay': routeData.median_delay,
                'vehicle_count': routeData.vehicle_count,
                'very_early_count': routeData.very_early_count,
                'very_late_count': routeData.very_late_count
            }
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
