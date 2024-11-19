# hello_cassandra.py
# If everything is set up correctly, this will get some metadata from our keyspaces

from cassandra.cluster import Cluster
from ssl import SSLContext, PROTOCOL_TLSv1_2 , CERT_REQUIRED
import boto3
from cassandra_sigv4.auth import SigV4AuthProvider
import sys

ssl_context = SSLContext(PROTOCOL_TLSv1_2)
ssl_context.load_verify_locations('../data/sf-class2-root.crt')
ssl_context.verify_mode = CERT_REQUIRED

# use this if you want to use Boto to set the session parameters.
boto_session = boto3.Session(aws_access_key_id=sys.argv[1],
                             aws_secret_access_key=sys.argv[2],
                             aws_session_token=sys.argv[3],
                             region_name="ca-central-1")
auth_provider = SigV4AuthProvider(boto_session)

cluster = Cluster(['cassandra.ca-central-1.amazonaws.com'], ssl_context=ssl_context, auth_provider=auth_provider,
                  port=9142)
session = cluster.connect()
r = session.execute('select * from system_schema.keyspaces')
print(r.current_rows)