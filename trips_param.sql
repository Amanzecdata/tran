CREATE TABLE trips_param (keys VARCHAR,
						  vals VARCHAR);

INSERT INTO trips_param (keys, vals)
VALUES ('file_part_name', 'aman_'),
        ('partition_num', '100'),
        ('full_path_with_fileName', '/home/hp/Desktop/chintu/tran/n26f-ihde.csv'),
	    ('only_file_path', '/home/hp/Desktop/chintu/tran/'),
	    ('splitted_csv_path', '/home/hp/Desktop/chintu/tran/'),
	    ('file_name', 'n26f-ihde.csv'),
	    ('file_extension', 'csv'),
	    ('tableName', 'trips_data');
		
select vals from trips_param where keys='splitted_csv_path';
		
select * from trips_param;



CREATE TABLE trips_data(trip_id Text, 
			   trip_start_timestamp timestamp, 
			   trip_end_timestamp timestamp, 
			   trip_seconds Numeric,
			   trip_miles Numeric, 
			   percent_time_chicago Numeric, 
			   percent_distance_chicago Numeric, 
			   pickup_census_tract Text, 
			   dropoff_census_tract Text,
               pickup_community_area Numeric, 
			   dropoff_community_area Numeric, 
			   fare Numeric, 
			   tip Numeric, 
			   additional_charges Numeric, 
			   trip_total Numeric,
			   shared_trip_authorized Boolean, 
			   shared_trip_match Boolean, 
			   trips_pooled Numeric, 
			   pickup_centroid_latitude Numeric, 
			   pickup_centroid_longitude Numeric, 
			   pickup_centroid_location VARCHAR, 
			   dropoff_centroid_latitude Numeric, 
			   dropoff_centroid_longitude Numeric,
			   dropoff_centroid_location VARCHAR);
			   
select * from trips_data where trip_id = 'f16ff76280e668f64a843150513c1880041ecb0b'

select count(*) from trips_data

truncate table trips_data