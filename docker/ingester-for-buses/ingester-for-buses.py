import json
from datetime import datetime, timezone
import urllib.parse
import boto3
from cassandra.cluster import Cluster, DCAwareRoundRobinPolicy
from ssl import SSLContext, PROTOCOL_TLSv1_2 , CERT_REQUIRED
from cassandra_sigv4.auth import SigV4AuthProvider
from cassandra.query import ConsistencyLevel
from cassandra.concurrent import execute_concurrent_with_args


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
    session.default_timeout = 60
    session.default_consistency_level = ConsistencyLevel.LOCAL_QUORUM
    return session


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


def read_position_update(json_string, upload_time):
    results = []
    data = json.loads(json_string)
    for update in data:
        update = json.loads(update)['vehicle']
        params = (
            update['vehicle']['id'],
            update['vehicle']['label'],
            update['trip']['routeId'],
            update['trip']['directionId'],
            update['currentStatus'],
            update['currentStopSequence'],
            update['stopId'],
            update['position']['latitude'],
            update['position']['longitude'],
            datetime.fromtimestamp(int(update['timestamp']), tz=timezone.utc),
            upload_time
        )
        results.append(params)
    return results


def ingest_position_update(session, position_params):
    print(f"Ingesting {len(position_params)} records to vehicle_by_route")
    insert_statement = session.prepare(
        """
        INSERT INTO vehicle_by_route(
            vehicle_id,
            vehicle_label,
            route_id,
            direction_id,
            current_status,
            stop_sequence,
            stop_id,
            latitude,
            longitude,
            last_update,
            update_time
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    )
    results = execute_concurrent_with_args(session, insert_statement, position_params)
    for (success, result) in results:
        if not success:
            print("ERROR: ", result)


def lambda_handler(event, context):
    try:
        json_string, upload_time = get_string_and_upload_time(event)

        session = create_session()
        
        print("Reading position updates from update file...")
        params = read_position_update(json_string, upload_time)
    
        print("Beginning ingestion...")
        ingest_position_update(session, params)
        
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
