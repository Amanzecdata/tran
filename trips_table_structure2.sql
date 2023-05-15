UPDATE trips_stage12 SET trip_start_timestamp = to_timestamp(trip_start_timestamp, 'MM/DD/YYYY HH:MI:SS AM'), trip_end_timestamp = to_timestamp(trip_end_timestamp, 'MM/DD/YYYY HH:MI:SS AM');

CREATE TABLE trips_stage2(id Text, 
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

CREATE TABLE trips_dim2( created timestamp default current_timestamp,
				updated timestamp default current_timestamp,
				is_active int default 1,
				is_deleted int default 0,
			   	id Text, 
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

CREATE TABLE trips_final2( created timestamp default current_timestamp,
				updated timestamp default current_timestamp,
				is_active int default 1,
				is_deleted int default 0,
	           id Text, 
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




SELECT column_name, data_type, is_nullable, column_default
FROM information_schema.columns
WHERE table_name = 'trips_stage12' AND column_name = 'trip_start_timestamp';

select * from trips_stage1;

select count(*) from trips_stage12;
select * from trips_stage12;

select * from trips_dim12;
select * from trips_final12;

truncate table trips_stage12

drop table trips_stage12

UPDATE trips_stage12 
SET trip_start_timestamp = date(trip_start_timestamp, 'MM/DD/YYYY HH:MI:SS AM'), 
trip_end_timestamp = date(trip_end_timestamp, 'MM/DD/YYYY HH:MI:SS AM')
