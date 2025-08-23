software_version_insert = """
    INSERT INTO laddms.software_version(
    	device_id, poll_timestamp,
    	commit_id, commit_timestamp, commit_timestamp_unix,
    	count, version_major, version_minor, version_patch, release_tag
	    )
	VALUES (
    	%(device_id)s, %(poll_timestamp)s,
    	%(commit_id)s, %(commit_timestamp)s, %(commit_timestamp_unix)s,
    	%(count)s, %(version_major)s, %(version_minor)s, %(version_patch)s, %(release_tag)s
	    );
"""

event_zone_insert = """
    INSERT INTO laddms.event_zones(
    	zone_id, poll_timestamp, zone_name, zone_type, device_id, has_point_count, 
    	clear_buffer_sec, clear_occupancy, event_buffer_sec, event_occupancy, 
        min_event_sec, max_event_sec, severity, event_classes,
    	include_classes, filter_classes, filter_min_height, filter_max_height,
    	min_height, max_height, vertex_x, vertex_y,
	    vertex_lon, vertex_lat, vertex_geom
	    )
	VALUES (
    	%(zone_id)s, %(poll_timestamp)s, %(zone_name)s, %(zone_type)s, %(device_id)s, %(has_point_count)s,
    	%(clear_buffer_sec)s, %(clear_occupancy)s, %(event_buffer_sec)s, %(event_occupancy)s,
        %(min_event_sec)s, %(max_event_sec)s, %(severity)s, %(event_classes)s,
    	%(include_classes)s, %(filter_classes)s, %(filter_min_height)s, %(filter_max_height)s,
    	%(min_height)s, %(max_height)s, %(vertex_x)s, %(vertex_y)s,
	    %(vertex_lon)s, %(vertex_lat)s, ST_GeomFromText(%(vertex_geom_text)s)
	    );
"""
# Input for vertex_geom_text should look like 'POLYGON((-71.177 42.390,-71.177 42.391, ...))'

point_zone_insert = """
    INSERT INTO laddms.point_zones(
    	zone_id, poll_timestamp, zone_name, zone_type, device_id, has_point_count,
    	metadata, min_height, max_height, vertex_x, vertex_y
	    )
	VALUES (
    	%(zone_id)s, %(poll_timestamp)s, %(zone_name)s, %(zone_type)s, %(device_id)s, %(has_point_count)s,
    	%(metadata)s, %(min_height)s, %(max_height)s, %(vertex_x)s, %(vertex_y)s
	    );
"""

zone_occupation_insert = """
    INSERT INTO laddms.zone_occupations(
    	occupation_timestamp, zone_id, zone_name, device_id, num_objects, num_points,
    	object_index,
    	object_id, object_uuid, update_timestamp, creation_timestamp, object_frame_count,
    	classification, sub_classification, velocity_x, velocity_y, velocity_z
	    )
	VALUES (
    	%(occupation_timestamp)s, %(zone_id)s, %(zone_name)s, %(device_id)s, %(num_objects)s, %(num_points)s,
    	%(object_index)s,
    	%(object_id)s, %(object_uuid)s, %(update_timestamp)s, %(creation_timestamp)s, %(object_frame_count)s,
    	%(classification)s, %(sub_classification)s, %(velocity_x)s, %(velocity_y)s, %(velocity_z)s
	    );
"""
# Input for object_index should be created by enumeration of the objects in the zone detection

object_detection_insert = """
    INSERT INTO laddms.broadway_object_detections(
    	frame_timestamp, frame_count, device_id,
    	object_id, object_uuid, update_timestamp, classification, sub_classification,
    	length, width, height, position_x, position_y, position_z,
    	velocity_x, velocity_y, velocity_z,
    	heading, orient_qw, orient_qx, orient_qy, orient_qz,
    	creation_timestamp, object_frame_count, initial_x, initial_y, initial_z,
    	primary_sensor, distance_primary_sensor,
    	num_total_points, num_primary_points, num_failed_returns, class_confidence,
    	uncertain_x, uncertain_y, uncertain_z, uncertain_vx, uncertain_vy, uncertain_vz,
    	frame_timestamp_dt, intersection_id
	    )
	VALUES (
    	%(frame_timestamp)s, %(frame_count)s, %(device_id)s,
    	%(object_id)s, %(object_uuid)s, %(update_timestamp)s, %(classification)s, %(sub_classification)s,
    	%(length)s, %(width)s, %(height)s, %(position_x)s, %(position_y)s, %(position_z)s,
    	%(velocity_x)s, %(velocity_y)s, %(velocity_z)s,
    	%(heading)s, %(orient_qw)s, %(orient_qx)s, %(orient_qy)s, %(orient_qz)s,
    	%(creation_timestamp)s, %(object_frame_count)s, %(initial_x)s, %(initial_y)s, %(initial_z)s,
    	%(primary_sensor)s, %(distance_primary_sensor)s,
    	%(num_total_points)s, %(num_primary_points)s, %(num_failed_returns)s, %(class_confidence)s,
    	%(uncertain_x)s, %(uncertain_y)s, %(uncertain_z)s, %(uncertain_vx)s, %(uncertain_vy)s, %(uncertain_vz)s
    	%(frame_timestamp_dt)s, %(intersection_id)
	    );
"""

