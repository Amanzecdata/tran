select count(*) from trips_stage_table

trip_seconds Numeric,
trip_miles Numeric,
pickup_census_tract Text,
shared_trip_match Boolean,

CREATE TABLE IF NOT EXISTS trips_stage_table (trip_id TEXT, trip_start_timestamp TEXT, trip_end_timestamp TEXT, trip_seconds TEXT, trip_miles TEXT, percent_time_chicago TEXT, percent_distance_chicago TEXT, pickup_census_tract TEXT, dropoff_census_tract TEXT, pickup_community_area TEXT, dropoff_community_area TEXT, fare TEXT, tip TEXT, additional_charges TEXT, trip_total TEXT, shared_trip_authorized TEXT, shared_trip_match TEXT, trips_pooled TEXT, pickup_centroid_latitude TEXT, pickup_centroid_longitude TEXT, pickup_centroid_location TEXT, dropoff_centroid_latitude TEXT, dropoff_centroid_longitude TEXT, dropoff_centroid_location TEXT);

drop table trips_stage_table

select * from trips_stage_table

select * from trips_dim_table

CREATE TABLE trips_dim_table(trip_id Text, 
			   trip_start_timestamp timestamp, 
			   trip_end_timestamp timestamp,  
			   percent_time_chicago Numeric, 
			   percent_distance_chicago Numeric, 
			   dropoff_census_tract Text,
               pickup_community_area Numeric, 
			   dropoff_community_area Numeric, 
			   fare Numeric, 
			   tip Numeric, 
			   additional_charges Numeric, 
			   trip_total Numeric,
			   shared_trip_authorized Boolean,  
			   trips_pooled Numeric, 
			   pickup_centroid_latitude Numeric, 
			   pickup_centroid_longitude Numeric, 
			   pickup_centroid_location VARCHAR, 
			   dropoff_centroid_latitude Numeric, 
			   dropoff_centroid_longitude Numeric,
			   dropoff_centroid_location VARCHAR);

CREATE TABLE trips_final_table(trip_id Text, 
			   trip_start_timestamp timestamp, 
			   trip_end_timestamp timestamp,  
			   percent_time_chicago Numeric, 
			   percent_distance_chicago Numeric, 
			   dropoff_census_tract Text,
               pickup_community_area Numeric, 
			   dropoff_community_area Numeric, 
			   fare Numeric, 
			   tip Numeric, 
			   additional_charges Numeric, 
			   trip_total Numeric,
			   shared_trip_authorized Boolean,  
			   trips_pooled Numeric, 
			   pickup_centroid_latitude Numeric, 
			   pickup_centroid_longitude Numeric, 
			   pickup_centroid_location VARCHAR, 
			   dropoff_centroid_latitude Numeric, 
			   dropoff_centroid_longitude Numeric,
			   dropoff_centroid_location VARCHAR)
			   created timestamp,
			   updated timestamp,
			   is_active int,
			   is_deleted int;



INSERT INTO trips_dim_table (trip_id, trip_start_timestamp, trip_end_timestamp, percent_time_chicago, percent_distance_chicago, dropoff_census_tract, pickup_community_area, dropoff_community_area, fare, tip, additional_charges, trip_total, shared_trip_authorized, trips_pooled, pickup_centroid_latitude, pickup_centroid_longitude, pickup_centroid_location, dropoff_centroid_latitude, dropoff_centroid_longitude, dropoff_centroid_location, created, updated, is_active, is_deleted)
SELECT CAST(s.trip_id AS text), CAST(s.trip_start_timestamp AS timestamp), CAST(s.trip_end_timestamp AS timestamp), CAST(s.trip_seconds AS numeric), CAST(s.trip_miles AS numeric), CAST(s.percent_time_chicago AS text), CAST(s.percent_distance_chicago AS numeric), CAST(s.pickup_census_tract AS numeric), CAST(s.dropoff_census_tract AS numeric), CAST(s.pickup_community_area AS numeric), CAST(s.dropoff_community_area AS numeric), CAST(s.fare AS numeric), CAST(s.shared_trip_match AS boolean), CAST(s.trips_pooled AS numeric), CAST(s.pickup_centroid_latitude AS numeric), CAST(s.pickup_centroid_longitude AS numeric), CAST(s.pickup_centroid_location AS varchar), CAST(s.dropoff_centroid_latitude AS numeric), CAST(s.dropoff_centroid_longitude AS numeric), CAST(s.dropoff_centroid_location AS varchar), now(), now(), 1, 0
FROM trips_stage_table s 
LEFT JOIN trips_dim_table t ON t.trip_id = s.trip_id WHERE t.trip_id = s.trip_id AND
(CAST(s.trip_start_timestamp AS timestamp)<>t.trip_start_timestamp OR CAST(s.trip_end_timestamp AS timestamp)<>t.trip_end_timestamp OR CAST(s.percent_time_chicago AS numeric)<>t.percent_time_chicago OR CAST(s.percent_distance_chicago AS numeric)<>t.percent_distance_chicago OR CAST(s.dropoff_census_tract AS text)<>t.dropoff_census_tract OR CAST(s.pickup_community_area AS numeric)<>t.pickup_community_area OR CAST(s.dropoff_community_area AS numeric)<>t.dropoff_community_area OR CAST(s.fare AS numeric)<>t.fare OR CAST(s.tip AS numeric)<>t.tip OR CAST(s.additional_charges AS numeric)<>t.additional_charges OR CAST(s.trip_total AS numeric)<>t.trip_total OR CAST(s.shared_trip_authorized AS boolean)<>t.shared_trip_authorized OR CAST(s.trips_pooled AS numeric)<>t.trips_pooled OR CAST(s.pickup_centroid_latitude AS numeric)<>t.pickup_centroid_latitude OR CAST(s.pickup_centroid_longitude AS numeric)<>t.pickup_centroid_longitude OR CAST(s.pickup_centroid_location AS varchar)<>t.pickup_centroid_location OR CAST(s.dropoff_centroid_latitude AS numeric)<>t.dropoff_centroid_latitude OR CAST(s.dropoff_centroid_longitude AS numeric)<>t.dropoff_centroid_longitude OR CAST(s.dropoff_centroid_location AS varchar)<>t.dropoff_centroid_location)
AND t.updated >= (SELECT MAX(created) FROM trips_dim_table WHERE t.trip_id = trips_dim_table.trip_id) AND is_active = 1;


INSERT INTO trips_dim_table (trip_id,trip_start_timestamp,trip_end_timestamp,percent_time_chicago,percent_distance_chicago,dropoff_census_tract,pickup_community_area,dropoff_community_area,fare,tip,additional_charges,trip_total,shared_trip_authorized,trips_pooled,pickup_centroid_latitude,pickup_centroid_longitude,pickup_centroid_location,dropoff_centroid_latitude,dropoff_centroid_longitude,dropoff_centroid_location)
SELECT ( s.trip_id,s.trip_start_timestamp,s.trip_end_timestamp,s.percent_time_chicago,s.percent_distance_chicago,s.dropoff_census_tract,s.pickup_community_area,s.dropoff_community_area,s.fare,s.tip,s.additional_charges,s.trip_total,s.shared_trip_authorized,s.trips_pooled,s.pickup_centroid_latitude,s.pickup_centroid_longitude,s.pickup_centroid_location,s.dropoff_centroid_latitude,s.dropoff_centroid_longitude,s.dropoff_centroid_location)
FROM trips_stage_table s WHERE trips_dim_table.trip_id IS NULL;

-------------------- TRIPS_CONFIG_TABLE --------------------------		

select * from trips_config
select * from trips_stage_table
select * from trips_stage where trip_id = '09f7cd978cecd8d45fb72a6c50c90465a538fa74'
select * from trips_dim_table where trip_id = '00a0ea7bc0a26f774ea11d04a7da81d5c65fef5a'



INSERT INTO trips_dim_table (trip_id, trip_start_timestamp, trip_end_timestamp, percent_time_chicago, percent_distance_chicago, dropoff_census_tract, pickup_community_area, dropoff_community_area, fare, tip, additional_charges, trip_total, shared_trip_authorized, trips_pooled, pickup_centroid_latitude, pickup_centroid_longitude, pickup_centroid_location, dropoff_centroid_latitude, dropoff_centroid_longitude, dropoff_centroid_location, created, updated, is_active, is_deleted)
SELECT CAST(s.trip_id AS text), CAST(s.trip_start_timestamp AS timestamp), CAST(s.trip_end_timestamp AS timestamp), CAST(s.trip_seconds AS numeric), CAST(s.trip_miles AS numeric), CAST(s.percent_time_chicago AS text), CAST(s.percent_distance_chicago AS numeric), CAST(s.pickup_census_tract AS numeric), CAST(s.dropoff_census_tract AS numeric), CAST(s.pickup_community_area AS numeric), CAST(s.dropoff_community_area AS numeric), CAST(s.fare AS numeric), CAST(s.shared_trip_match AS boolean), CAST(s.trips_pooled AS numeric), CAST(s.pickup_centroid_latitude AS numeric), CAST(s.pickup_centroid_longitude AS numeric), CAST(s.pickup_centroid_location AS varchar), CAST(s.dropoff_centroid_latitude AS numeric), CAST(s.dropoff_centroid_longitude AS numeric), CAST(s.dropoff_centroid_location AS varchar), now(), now(), 1, 0
FROM trips_stage_table s 
LEFT JOIN trips_dim_table t ON t.trip_id = s.trip_id WHERE t.trip_id = s.trip_id AND
(CAST(s.trip_id AS text)<>t.trip_id OR CAST(s.trip_start_timestamp AS timestamp)<>t.trip_start_timestamp OR CAST(s.trip_end_timestamp AS timestamp)<>t.trip_end_timestamp OR CAST(s.trip_seconds AS numeric)<>t.percent_time_chicago OR CAST(s.trip_miles AS numeric)<>t.percent_distance_chicago OR CAST(s.percent_time_chicago AS text)<>t.dropoff_census_tract OR CAST(s.percent_distance_chicago AS numeric)<>t.pickup_community_area OR CAST(s.pickup_census_tract AS numeric)<>t.dropoff_community_area OR CAST(s.dropoff_census_tract AS numeric)<>t.fare OR CAST(s.pickup_community_area AS numeric)<>t.tip OR CAST(s.dropoff_community_area AS numeric)<>t.additional_charges OR CAST(s.fare AS numeric)<>t.trip_total OR CAST(s.tip AS boolean)<>t.shared_trip_authorized OR CAST(s.additional_charges AS numeric)<>t.trips_pooled OR CAST(s.trip_total AS numeric)<>t.pickup_centroid_latitude OR CAST(s.shared_trip_authorized AS numeric)<>t.pickup_centroid_longitude OR CAST(s.shared_trip_match AS varchar)<>t.pickup_centroid_location OR CAST(s.trips_pooled AS numeric)<>t.dropoff_centroid_latitude OR CAST(s.pickup_centroid_latitude AS numeric)<>t.dropoff_centroid_longitude OR CAST(s.pickup_centroid_longitude AS varchar)<>t.dropoff_centroid_location OR CAST(s.pickup_centroid_location AS timestamp)<>t.pickup_centroid_location OR CAST(s.dropoff_centroid_latitude AS timestamp)<>t.dropoff_centroid_latitude OR CAST(s.dropoff_centroid_longitude AS int)<>t.dropoff_centroid_longitude OR CAST(s.dropoff_centroid_location AS int)<>t.dropoff_centroid_location)
AND t.updated >= (SELECT MAX(created) FROM trips_dim_table WHERE t.trip_id = trips_dim_table.trip_id) AND is_active = 1;

create table trips_config (output_file_path TEXT, output_file_name TEXT, 
						  target_table_name TEXT, source_table_name TEXT, 
						  config_file_path TEXT);
insert into trips_config (config_file_path,output_file_path, output_file_name, 
						  target_table_name, source_table_name,target_alias,source_alias, final_alias, final_table_name)
		VALUES ('/home/hp/Desktop/chintu/tran/trips_config.csv','/home/hp/Desktop/chintu/tran/', 'output.csv', 'trips_dim_table', 'trips_stage_table','t','s','f','trips_final_table');
		

alter table trips_config
add column final_alias varchar,
add column final_table_name varchar;		

truncate table trips_config
		
select * from trips_config

select * from trips_final_table

CREATE TABLE IF NOT EXISTS trips_final_table (trip_id text, trip_start_timestamp timestamp, trip_end_timestamp timestamp, percent_time_chicago numeric, percent_distance_chicago numeric, dropoff_census_tract text, pickup_community_area numeric, dropoff_community_area numeric, fare numeric, tip numeric, additional_charges numeric, trip_total numeric, shared_trip_authorized boolean, trips_pooled numeric, pickup_centroid_latitude numeric, pickup_centroid_longitude numeric, pickup_centroid_location varchar, dropoff_centroid_latitude numeric, dropoff_centroid_longitude numeric, dropoff_centroid_location varchar, created timestamp, updated timestamp, is_active int, is_deleted int);		
drop table trips_config

-------------------------------------------------------------------

truncate table trips_stage_table
truncate table trips_dim_table
truncate table trips_final_table


select* from trips_stage_table
select* from trips_dim_table
select* from trips_final_table


drop table trips_stage_table
drop table trips_final_table
drop table trips_dim_table