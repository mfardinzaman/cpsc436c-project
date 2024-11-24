# upload_static_data.py

import os
import csv
from cassandra.cluster import Cluster
from ssl import SSLContext, PROTOCOL_TLSv1_2 , CERT_REQUIRED
import boto3
from cassandra_sigv4.auth import SigV4AuthProvider
from cassandra.query import BatchStatement, ConsistencyLevel, BatchType


def create_session(access_key_id, secret_access_key, session_token):
    ssl_context = SSLContext(PROTOCOL_TLSv1_2)
    ssl_context.load_verify_locations('../data/sf-class2-root.crt')
    ssl_context.verify_mode = CERT_REQUIRED

    boto_session = boto3.Session(aws_access_key_id=access_key_id,
                                aws_secret_access_key=secret_access_key,
                                aws_session_token=session_token,
                                region_name='ca-central-1')
    auth_provider = SigV4AuthProvider(boto_session)

    cluster = Cluster(['cassandra.ca-central-1.amazonaws.com'], ssl_context=ssl_context, auth_provider=auth_provider,
                    port=9142)
    session = cluster.connect(keyspace='Translink')
    return session


def create_route_table(session):
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS Route (
            route_id varchar,
            route_short_name varchar,
            route_long_name varchar,
            route_type int,
            direction_id int,
            direction varchar,
            direction_name varchar,            
            PRIMARY KEY (route_id, direction_id)
        );
        """
    )
    
def create_stop_table(session):
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS Stop(
            stop_id varchar,
            stop_code varchar,
            stop_name varchar,
            latitude float,
            longitude float,
            zone_id varchar,
            location_type int,
            wheelchair_boarding int,
            PRIMARY KEY (stop_id)
        );
        """
    )
    

def create_route_statistic_tables(session, test=False):
    test_label = "_test" if test else ""
    session.execute(
        f"""
        CREATE TABLE IF NOT EXISTS route_stat_by_route{test_label}(
            route_id varchar,
            direction_id int,
            average_delay int,
            median_delay int,
            very_early_count int,
            very_late_count int,
            vehicle_count int,
            update_time timestamp,
            PRIMARY KEY (route_id, direction_id, update_time)
        );
        """
    )
    session.execute(
        f"""
        CREATE TABLE IF NOT EXISTS route_stat_by_time{test_label}(
            route_id varchar,
            route_short_name varchar,
            route_long_name varchar,
            route_type int,
            direction_id int,
            direction varchar,
            direction_name varchar,
            average_delay int,
            median_delay int,
            very_early_count int,
            very_late_count int,
            vehicle_count int,
            update_time timestamp,
            PRIMARY KEY (update_time, route_id, direction_id)
        );
        """
    )
    
    
def create_batch():
    return BatchStatement(batch_type=BatchType.UNLOGGED, consistency_level=ConsistencyLevel.LOCAL_QUORUM)

    
def populate_route_table(session):
    directions = {}
    with open('../data/directions.txt', 'r') as f:
        heading = next(f)
        csv_reader = csv.reader(f)
        for row in csv_reader:
            direction = row[0]
            direction_id = row[1]
            route_id = row[2]
            direction_info = (direction_id, direction)
            try:
                directions[route_id].append(direction_info)
            except KeyError:
                directions[route_id] = [direction_info]
                
    direction_names = {}
    with open('../data/direction_names_exceptions.txt', 'r') as f:
        heading = next(f)
        csv_reader = csv.reader(f)
        for row in csv_reader:
            route_short_name_unpadded = row[0]
            direction_id = row[1]
            direction_name = row[2]
            direction_names[(route_short_name_unpadded, direction_id)] = direction_name
    
                
    insert_user = session.prepare(
        """
        INSERT INTO Route (route_id, route_short_name, route_long_name, route_type, direction_id, direction, direction_name)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """
    )
    batch = create_batch()
    
    with open("../data/routes.txt", 'r') as f:
        heading = next(f)
        csv_reader = csv.reader(f)
        count = 0
        for row in csv_reader:
            route_id = row[0]
            route_short_name = row[2]
            route_long_name = row[3]
            route_type = row[5]
            if route_short_name == "":
                route_short_name = route_long_name
            try:
                for entry in directions[route_id]:
                    direction_id = entry[0]
                    direction = entry[1]
                    direction_name = None
                    try:
                        direction_name = direction_names[(route_short_name.lstrip('0'), direction_id)]
                        # print(route_id, route_short_name, route_long_name, route_type, direction_id, direction, direction_name)
                        try:
                            if count == 30:
                                session.execute(batch)
                                batch = create_batch()
                                count = 0
                            batch.add(insert_user, (route_id, route_short_name, route_long_name, int(route_type), int(direction_id), direction, direction_name))
                            count += 1
                        except Exception as e:
                            print(f"Cassandra error: {e}")                            
                    except KeyError:
                        print("Direction name KeyError:", route_id, route_short_name, route_long_name, route_type, direction_id, direction)
            except KeyError:
                print("Direction KeyError:", route_id, route_short_name, route_long_name, route_type)
                continue
    
    session.execute(batch)


def populate_stop_table(session):
    insert_user = session.prepare(
        """
        INSERT INTO Stop (stop_id, stop_code, stop_name, latitude, longitude, zone_id, location_type, wheelchair_boarding)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
    )
    
    with open('../data/stops.txt', 'r') as f:
        heading = next(f)
        csv_reader = csv.reader(f)
        count = 0
        batch = create_batch()
        for row in csv_reader:
            stop_id, stop_code, stop_name, stop_desc, stop_lat, stop_lon, \
                zone_id, stop_url, location_type, parent_station, \
                    wheelchair_boarding = row
            try:
                if count == 30:
                    session.execute(batch)
                    batch = create_batch()
                    count = 0
                batch.add(insert_user, (stop_id, stop_code, stop_name, float(stop_lat), float(stop_lon), 
                                        zone_id, int(location_type), int(wheelchair_boarding)))
                count += 1
            except Exception as e:
                print(f"Cassandra error: {e}")  
            
    session.execute(batch)


def drop_table(session, table):
    session.execute(f"DROP TABLE IF EXISTS {table}")


def list_tables(session):
    r = session.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'Translink';")
    print(r.current_rows)
    

def list_route_rows(session):
    r = session.execute("SELECT * FROM route;")
    for row in r.current_rows:
        print(row)
    

if __name__ == "__main__":
    session = create_session(os.getenv('AWS_ACCESS_KEY_ID'), os.getenv('AWS_SECRET_ACCESS_KEY'), os.getenv('AWS_SESSION_TOKEN'))
    create_route_statistic_tables(session, test=True)
    # create_route_table(session)
    # create_stop_table(session)
    # populate_route_table(session)
    # populate_stop_table(session)
    # drop_table(session, 'route_stat_by_route')
    # list_tables(session)
    # list_route_rows(session)