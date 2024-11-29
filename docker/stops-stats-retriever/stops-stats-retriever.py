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

def get_stops_stats(session, update_time):
    # prepared = session.prepare("SELECT * FROM stop_stat_by_time WHERE day = ? AND update_time = ?")
    prepared = session.prepare("SELECT stop_id, stop_name, stop_code, average_delay, very_late_count, stop_count FROM stop_stat_by_time WHERE day = ? AND update_time = ?")
    bound = prepared.bind((update_time.date(), update_time))
    results = session.execute(bound)
    return results

# def get_stops_stats(session):
#     query = create_statement(f"SELECT * FROM stop_stat_by_time WHERE day = '{datetime.date(datetime.today())}' AND update_time > '{datetime.date(datetime.now()) - timedelta(hours=1)}';")
#     results = session.execute(query)
#     results = results.all()
#     if (len(results) == 0):
#         query = create_statement(f"SELECT * FROM stop_stat_by_time WHERE day = '{datetime.date(datetime.today() - timedelta(days=1))}' AND update_time > '{datetime.date(datetime.now()) - timedelta(hours=1)}';")
#         results = session.execute(query)

#     return results


def lambda_handler(event, context):
    try:
        session = create_session()

        update_time = get_last_update_time(session)
        stops = get_stops_stats(session, update_time)


        results = []
        # latestSet = False
        for stopData in stops:
            # if (latestSet == False):
            #     latestUpdate = stopData.update_time
            #     latestSet = True
            # elif (stopData.update_time < latestUpdate):
            #     break
            # result = {
            #     'zone_id': stopData.zone_id,
            #     'stop_id': stopData.stop_id,
            #     'update_time': stopData.update_time.isoformat(),
            #     'average_delay': stopData.average_delay,
            #     'latitude': stopData.latitude,
            #     'longitude': stopData.longitude,
            #     'location_type': stopData.location_type,
            #     'median_delay': stopData.median_delay,
            #     'stop_code': stopData.stop_code,
            #     'stop_count': stopData.stop_count,
            #     'stop_name': stopData.stop_name,
            #     'wheelchair_boarding': stopData.wheelchair_boarding,
            #     'very_early_count': stopData.very_early_count,
            #     'very_late_count': stopData.very_late_count
            # }
            # stopData.update_time = str stopData.update_time.isoformat())
            results.append(stopData)

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
