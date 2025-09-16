from os import times

import psycopg
from queries_insert import *

import os
import time
import warnings
from datetime import datetime
from zoneinfo import ZoneInfo
import pytz

def make_sql_connection(database_name: str = 'NDOT',
                        autocommit: bool = True, retry: bool = True,
#                        override_env_config: dict | None = None) -> psycopg.Connection:
                        override_env_config: dict = None) -> psycopg.Connection:
    """
    Create a persistent database connection and set its timezone.
    :param database_name: specific name of database
    :param autocommit: T/F for connection autocommit setting
    :param retry: T/F for retry connection multiple times before giving up
    :param override_env_config: (optional) dictionary of configuration values, as alternative to .env configuration;
        must include keys = SQL_HOSTNAME, SQL_PORT, SQL_USERNAME, SQL_PASSWORD
    :return: psycopg.Connection instance
    """

    # Initialize counter based on function call argument
    if retry is True:
        retry_counter = 5
    else:
        retry_counter = 1

    if override_env_config is not None:
        assert isinstance(override_env_config, dict), "Must provide a pre-loaded dict of connection config values."
        hostname = override_env_config.get('SQL_HOSTNAME')
        port = override_env_config.get('SQL_PORT', 5432)
        username = override_env_config.get('SQL_USERNAME')
        password = override_env_config.get('SQL_PASSWORD')
    else:
        hostname = os.environ.get('SQL_HOSTNAME')
        port = os.environ.get('SQL_PORT', 5432)
        username = os.environ.get('SQL_USERNAME')
        password = os.environ.get('SQL_PASSWORD')
    stripped_hostname = str(hostname).strip().strip('"').strip("'")
    # print("hostname: " + stripped_hostname)
    # print("port: " + str(port))
    # print("username: " + str(username))
    # print("password: " + str(password))
    # Attempt connection and retry if unsuccessful, up to the retry limit
    connection_error_context = None
    while retry_counter > 0:
        try:
            conn = psycopg.connect(host=stripped_hostname, port=port, user=username, password=password,
                                   dbname=database_name, autocommit=autocommit)
            break
        except psycopg.OperationalError as e:
            connection_error_context = e
            print("WARNING: Could not connect database. Trying again....")
            retry_counter -= 1
            time.sleep(2)
    else:
        warnings.warn(f"Database parameters used were: "
                      f"host={hostname}, port={port}, dbname={database_name}, user={username}")
        raise connection_error_context

    return conn
    

def make_sql_cursor(db_conn: psycopg.Connection) -> psycopg.Cursor:
    """

    :param db_conn: open database connection object (psycopg.Connection)
    :return: psycopg.Cursor object with correct row factory installed
    """
    return db_conn.cursor()


def insert_software_version(json_data, device_id: str,
                            use_db_cursor: psycopg.Cursor):
    query_params = {
    	device_id: device_id,
        # This is the timestamp when the API call was made and data was received
        'poll_timestamp': None,
    	'commit_id': None,
        'commit_timestamp': None,
        'commit_timestamp_unix': None,
    	'count': None,
        'version_major': None,
        'version_minor': None,
        'version_patch': None,
        'release_tag': None,
    }

    use_db_cursor.execute(query_text=software_version_insert,
                          query_parameters=query_params)
    return


def insert_event_zones(json_data, device_id: str,
                       use_db_cursor: psycopg.Cursor):
    
    list_of_params = []
    UTC = pytz.utc
    poll_timestamp = datetime.now(UTC)
    for zone_json in json_data["zones"]:
        if not "event_occupancy" in zone_json["metadata"]:
            event_occpancy = float('nan')
        else:
            event_occpancy = zone_json["metadata"]["event_occupancy"]
            
        if not "min_event_length_seconds" in zone_json["metadata"]:
            min_eventlength_seconds = float('nan')
        else:
            min_eventlength_seconds = zone_json["metadata"]["min_event_length_seconds"]
            
        if not "max_recording_length_seconds" in zone_json["metadata"]:
            max_recording_length_seconds = float('nan')
        else:
            max_recording_length_seconds = zone_json["metadata"]["max_recording_length_seconds"]
            
        if not "severity" in zone_json["metadata"]:
            severity = ""
        else:
            severity = zone_json["metadata"]["severity"]
            
                        
        if not "event_recording" in zone_json["metadata"]:
            clear_buffer_sec = float('nan')
            clear_occupancy = float('nan')
            event_buffer_sec = float('nan')
            event_classes = []
        else:
            clear_buffer_sec = zone_json["metadata"]["event_recording"]["clear_buffer_seconds"]
            clear_occupancy = zone_json["metadata"]["event_recording"]["clear_occupancy"]
            event_buffer_sec = zone_json["metadata"]["event_recording"]["event_buffer_seconds"]
            event_classes = zone_json["metadata"]["event_recording"]["event_classifications"]
                        
        # Input for vertex_geom_text should look like 'POLYGON((-71.177 42.390,-71.177 42.391, ...))'
        vertices = ','.join([str(coord['x']) + ' ' + str(coord['y']) for coord in zone_json['vertices']])
        vertices = vertices + ',' + str(zone_json['vertices'][0]["x"]) + ' ' + str(zone_json['vertices'][0]["y"])
        vertex_geom_text = f"POLYGON(({vertices}))"
#        print("vertex_geom_text = " + vertex_geom_text)
        query_params = {
        	'zone_id': zone_json["id"],
            # This is the timestamp when the API call was made and data was received
            'poll_timestamp': poll_timestamp,
            'zone_name': zone_json["name"],
            'zone_type': zone_json["type"],
            'device_id': device_id,
            'has_point_count': zone_json["has_point_count"],
        	'clear_buffer_sec': clear_buffer_sec,
            'clear_occupancy': clear_occupancy,
            'event_buffer_sec': event_buffer_sec,
            'event_occupancy': event_occpancy,
            'min_event_sec': min_eventlength_seconds,
            'max_event_sec': max_recording_length_seconds,
            'severity': severity,
            'event_classes': event_classes,    # list
        	'include_classes': zone_json["metadata"]["include_classifications"],  # list
            'filter_classes': zone_json["metadata"]["object_filters"][0]["classification"],
            'filter_min_height': zone_json["metadata"]["object_filters"][0]["min_height"],
            'filter_max_height': zone_json["metadata"]["object_filters"][0]["max_height"],
        	'min_height': zone_json["min_height"],
            'max_height': zone_json["max_height"],
            'vertex_x': [v["x"] for v in zone_json["vertices"] ],         # list of x coordinates
            'vertex_y': [v["y"] for v in zone_json["vertices"] ],         # list of y coordinates
    	    'vertex_lon': [v["x"] for v in zone_json["geo_vertices"] ],       # list of geo x coordinates
            'vertex_lat': [v["y"] for v in zone_json["geo_vertices"] ],       # list of geo y coordinates
            # Input for vertex_geom_text should look like 'POLYGON((-71.177 42.390,-71.177 42.391, ...))'
            'vertex_geom_text': vertex_geom_text,
        }
        list_of_params.append(query_params)

#    use_db_cursor.executemany(query_text=event_zone_insert,
#                              query_parameters=list_of_params)
    use_db_cursor.executemany(event_zone_insert,
                              list_of_params)
    return
    

def insert_point_zones(json_data, device_id: str,
                       use_db_cursor: psycopg.Cursor):
    
    list_of_params = []
    for zone_json in json_data:
        query_params = {
        	'zone_id': None,
            # This is the timestamp when the API call was made and data was received
            'poll_timestamp': None,
        	'zone_name': None,
            'zone_type': None,
            'device_id': device_id,
            'has_point_count': None,
        	'metadata': None,
            'min_height': None,
            'max_height': None,
            'vertex_x': None,         # list of x coordinates
            'vertex_y': None,         # list of y coordinates
        }
        list_of_params.append(query_params)

    use_db_cursor.executemany(query_text=event_zone_insert,
                              query_parameters=list_of_params)
    return
    

def insert_zone_occupation(json_data, device_id: str,
                           use_db_cursor: psycopg.Cursor):
    # Parse message wide timestamp.
    this_timestamp = time.time()
    
    list_of_params = []
    for zone_json in json_data["occupations"][0]["occupations"]:
        # Parse zone summary info.
        this_zone_id = zone_json["id"]
        this_zone_name = zone_json["name"]
        this_num_objects = zone_json["num_objects"]
        this_num_points = zone_json["num_points"]
        
        for obj_i, object_json in enumerate(zone_json["objects"]):
            query_params = {
            	'occupation_timestamp': this_timestamp,
                'zone_id': this_zone_id,
                'zone_name': this_zone_name,
                'device_id': device_id,
                'num_objects': this_num_objects,
                'num_points': this_num_points,
            	'object_index': obj_i,
            	'object_id': object_json["id"],
                'object_uuid': object_json["uuid"],
                'update_timestamp': object_json["update_ts"],
                'creation_timestamp': object_json["creation_ts"],
                'object_frame_count': object_json["frame_count"],
            	'classification': object_json["classification"],
                'sub_classification': object_json["sub_classification"],
                'velocity_x': object_json["velocity"]["x"],
                'velocity_y': object_json["velocity"]["y"],
                'velocity_z': object_json["velocity"]["z"],
            }
            list_of_params.append(query_params)

#    use_db_cursor.executemany(query_text=zone_occupation_insert,
#                              query_parameters=list_of_params)
    use_db_cursor.executemany(zone_occupation_insert,
                              list_of_params)
    return


def insert_object_detections(intersection_id, timestamp_tz, json_data, device_id: str,
                             use_db_cursor: psycopg.Cursor = None,
                             use_db_conn: psycopg.Connection = None,
                             dict_with_object_list = None) -> (int, float):
    """
    Takes an object detections frame containing one or more individual objects and inserts each of them into the
        database using a batch insert.
    :param json_data: JSON string loaded as a Python dict containing a list of objects
    :param device_id:
    :param use_db_cursor:
    :return:
    """
    ti = time.time()
    close_cursor = False
    if use_db_cursor is None:
        if use_db_conn is None:
            raise ValueError("Didn't receive active DB connection or cursor.")
        else:
            use_db_cursor = use_db_conn.cursor()
            close_cursor = True

    if dict_with_object_list is not None and isinstance(dict_with_object_list, dict):
        parse_dict = dict_with_object_list
    else:
        if 'object_list' not in json_data or len(json_data["object_list"]) == 0:
            return 0, 0
        parse_dict = json_data["object_list"][0]

    if "timestamp" not in parse_dict or "frame_count" not in parse_dict:
        return 0, 0

    # Parse frame info one time.
    this_frame_timestamp = parse_dict["timestamp"]
    this_frame_count = parse_dict["frame_count"]

    if timestamp_tz is None:
        ts_utc = datetime.fromtimestamp(this_frame_timestamp / 1000000, tz=ZoneInfo("UTC"))
        timestamp_tz = ts_utc.astimezone(tz=ZoneInfo("US/Central"))
    
    list_of_params = []
    for object_json in parse_dict["objects"]:
        query_params = {
        	'frame_timestamp': this_frame_timestamp,
            'frame_count': this_frame_count,
            'device_id': device_id,
        	'object_id': object_json["id"],
            'object_uuid': object_json["uuid"],
            'update_timestamp': object_json["update_ts"],
            'classification': object_json["classification"],
            'sub_classification': object_json["sub_classification"],
        	'length': object_json["dimensions"]["length"],
            'width': object_json["dimensions"]["width"],
            'height': object_json["dimensions"]["height"],
            'position_x': object_json["position"]["x"],
            'position_y': object_json["position"]["y"],
            'position_z': object_json["position"]["z"],
        	'velocity_x': object_json["velocity"]["x"],
            'velocity_y': object_json["velocity"]["y"],
            'velocity_z': object_json["velocity"]["z"],
        	'heading': object_json["heading"],
            'orient_qw': object_json["orientation"]["qw"],
            'orient_qx': object_json["orientation"]["qx"],
            'orient_qy': object_json["orientation"]["qy"],
            'orient_qz': object_json["orientation"]["qz"],
        	'creation_timestamp': object_json["creation_ts"],
            'object_frame_count': object_json["frame_count"],
            'initial_x': object_json["initial_position"]["x"],
            'initial_y': object_json["initial_position"]["y"],
            'initial_z': object_json["initial_position"]["z"],
        	'primary_sensor': object_json["primary_sensor"],
            'distance_primary_sensor': object_json["distance_to_primary_sensor"],
        	'num_total_points': object_json["num_points"],
            'num_primary_points': object_json["num_points_from_primary_sensor"],
            'num_failed_returns': object_json["num_failed_returns"],
            'class_confidence': object_json["classification_confidence"],
        	'uncertain_x': object_json["position_uncertainty"]["x"],
            'uncertain_y': object_json["position_uncertainty"]["y"],
            'uncertain_z': object_json["position_uncertainty"]["z"],
            'uncertain_vx': object_json["velocity_uncertainty"]["x"],
            'uncertain_vy': object_json["velocity_uncertainty"]["y"],
            'uncertain_vz': object_json["velocity_uncertainty"]["z"],
            'frame_timestamp_dt': timestamp_tz,
            'intersection_id': intersection_id,
        }
        
        list_of_params.append(query_params)
    # print(f"Inserting {len(list_of_params)} objects into database.")
    use_db_cursor.executemany(object_detection_insert,
                              list_of_params)
    if close_cursor:
        use_db_cursor.close()
    return len(list_of_params), time.time() - ti
