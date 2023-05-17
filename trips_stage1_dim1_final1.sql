CREATE TABLE trips_stage1 (trip_id Text, 
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

CREATE TABLE trips_dim1 (  created timestamp default current_timestamp,
							updated timestamp default current_timestamp,
							is_active int default 1,
							is_deleted int default 0,
							trip_id Text, 	
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

CREATE TABLE trips_final1 ( created timestamp default current_timestamp,
							updated timestamp default current_timestamp,
							is_active int default 1,
							is_deleted int default 0,
							trip_id Text, 
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

select count(*) from trips_dim1

select count(*) from trips_final1

select * from trips_stage1;
select * from trips_dim1;
select * from trips_final1;
select * from trips_final2;

select * from trips_final1 where trip_id = '0000f4a746d9497e99324483a885b6bb50372361'

SELECT column_name, data_type, is_nullable, column_default
FROM information_schema.columns
WHERE table_name = 'trips_stage1';


select * from merge_final

truncate table trips_stage1;
truncate table trips_dim1;
truncate table trips_final1;

select * from trips_dim1 where trip_id = '2c5446182d2cfb9d2db0d502e3905ebca95139e9'

drop table trips_stage1;
drop table trips_dim1;
drop table trips_final1;


