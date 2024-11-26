import json
import statistics
from datetime import datetime
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

    
def get_routes(session):
    query = create_statement(f"SELECT * FROM stop;")
    results = session.execute_async(query)
    return results



def lambda_handler(event, context):
    try:
        session = create_session()
        stops = get_routes(session).result()

        results = []
        for stop in stops:
            results.append(stop)

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
