import json
import statistics
from datetime import datetime, timezone
import urllib.parse
import boto3
from cassandra.cluster import Cluster, DCAwareRoundRobinPolicy
from ssl import SSLContext, PROTOCOL_TLSv1_2 , CERT_REQUIRED
from cassandra_sigv4.auth import SigV4AuthProvider
from cassandra.query import SimpleStatement, BatchStatement, ConsistencyLevel, BatchType
from cassandra.concurrent import execute_concurrent, execute_concurrent_with_args 


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


def get_english(translations):
    for translation in translations:
        if translation['language'] == 'en':
            return translation['text']
    return ''


def get_datetime_from_timestamp(timestamp):
    return datetime.fromtimestamp(int(timestamp), tz=timezone.utc)


def read_alerts(json_string):
    results = []
    data = json.loads(json_string)
    for alert in data:
        alert = json.loads(alert)
        alert_details = alert['alert']
        header = get_english(alert_details['headerText']['translation'])
        description = get_english(alert_details['descriptionText']['translation'])
        try:
            start = get_datetime_from_timestamp(alert_details['activePeriod'][0]['start'])
        except KeyError:
            start = get_datetime_from_timestamp(0)
        try:
            end = get_datetime_from_timestamp(alert_details['activePeriod'][0]['end'])
        except KeyError:
            end = datetime(year=2100, month=1, day=1)
        params = (
            alert['id'],
            start,
            end,
            alert_details['cause'],
            alert_details['effect'],
            header,
            description,
            alert_details['severityLevel']
        )
        results.append(params)
    return results


def ingest_alerts(session, params):
    statement = session.prepare(
        """
        INSERT INTO alert(alert_id, start, end, cause, effect, header, description, severity_level)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
    )
    results = execute_concurrent_with_args(session, statement, params)
    for (success, result) in results:
        if not success:
            print("ERROR: ", result)


def lambda_handler(event, context):
    try:
        json_string, upload_time = get_string_and_upload_time(event)

        session = create_session()
        
        print("Reading alert updates from update file...")
        params = read_alerts(json_string)
        
        print("Beginning ingestion...")
        ingest_alerts(session, params)
        
        print("Ingestion complete!")

        return {
            'statusCode': 200,
            'body': 'Success'
        }
    except Exception as err:
        print("ERROR:", err)

    return {
        'statusCode': 400,
        'body': 'Failure'
    }
