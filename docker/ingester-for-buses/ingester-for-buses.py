import json
from datetime import datetime, timezone, timedelta
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


def read_position_update(json_string):
    results = {}
    data = json.loads(json_string)
    for update in data:
        update = json.loads(update)['vehicle']
        results[(update['stopId'], update['trip']['tripId'])] = update
    return results


def retrieve_most_recent_update_time(session, update_time):
    select_statement = session.prepare("SELECT * FROM update_time WHERE day = ? LIMIT 1")
    today = update_time.date()
    yesterday = (update_time - timedelta(days=1)).date()
    params = ((today,), (yesterday,))
    results = execute_concurrent_with_args(session, select_statement,  params)
    
    t = None
    for (success, result) in results:
        if not success:
            print("ERROR: Failed to get latest update time:", result)
        else:
            row = result.one()
            if t is None or row.day == today:
                t = row.update_time
    return t


def retrieve_delays(session, updates, update_time):
    delays = {}
    t = update_time.isoformat(timespec="milliseconds")
    select_statement = session.prepare(f"SELECT stop_id, trip_id, delay FROM stop_update WHERE stop_id = ? AND trip_id = ? AND update_time = '{t}'")
    results = execute_concurrent_with_args(session, select_statement, updates.keys(), concurrency=50)
    for (success, result) in results:
        if not success:
            print("ERROR: ", result)
        else:
            row = result.one()
            if row is None:
                continue
            delays[(row.stop_id, row.trip_id)] = row.delay
    return delays


def generate_position_params(updates, delays, upload_time):
    results = []
    for key, update in updates.items():
        try:
            delay = delays[key]
        except KeyError:
            delay = None
        params = (
            update['vehicle']['id'],
            update['vehicle']['label'],
            update['trip']['tripId'],
            update['trip']['routeId'],
            update['trip']['directionId'],
            update['currentStatus'],
            update['currentStopSequence'],
            update['stopId'],
            update['position']['latitude'],
            update['position']['longitude'],
            delay,
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
            trip_id,
            route_id,
            direction_id,
            current_status,
            stop_sequence,
            stop_id,
            latitude,
            longitude,
            delay,
            last_update,
            update_time
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        updates = read_position_update(json_string)
        
        print("Retrieving the most recent update time...")
        update_time = retrieve_most_recent_update_time(session, upload_time)
        
        print(f"Retrieving {len(updates)} delay records from stop_updates at update_time {update_time.isoformat()}...")
        delays = retrieve_delays(session, updates, update_time)
        
        print("Generating parameters for ingestion...")
        params = generate_position_params(updates, delays, upload_time)
    
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
