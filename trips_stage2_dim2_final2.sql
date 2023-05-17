CREATE TABLE trips_stage2(trip_id Text, 
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

CREATE TABLE trips_final2( created timestamp default current_timestamp,
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

SET datestyle = 'ISO, MDY';


insert into trips_stage2 (trip_id , trip_start_timestamp , trip_end_timestamp , 
			   trip_seconds ,trip_miles , percent_time_chicago , percent_distance_chicago , 
			   pickup_census_tract , dropoff_census_tract ,pickup_community_area , 
			   dropoff_community_area , fare , tip , additional_charges , 
			   trip_total ,shared_trip_authorized , shared_trip_match , 
			   trips_pooled , pickup_centroid_latitude , 
			   pickup_centroid_longitude , pickup_centroid_location , 
			   dropoff_centroid_latitude , dropoff_centroid_longitude ,
			   dropoff_centroid_location)
Values ('000003a87a9701461872f5a6988b0edea7a1dcba',' 01/21/2023 09:45:00 PM', '01/21/2023 10:00:00 PM', 892, 3.6, 1, 1, 17031830700, 17031831800, 3, 13, 10, 0, 2.78, 12.78, false, false, 1, 41.958055933, -87.6603894557, 'POINT (-87.6603894557 41.958055933)', 41.9782942489, -87.7164304157, 'POINT (-87.7164304157 41.9782942489)');




SELECT column_name, data_type, is_nullable, column_default
FROM information_schema.columns
WHERE table_name = 'trips_stage2' AND column_name = 'trip_start_timestamp';

select * from trips_stage2;

truncate table trips_stage2;
select * from trips_dim2;


select * from trips_final2 where trip_id = '0000f4a746d9497e99324483a885b6bb50372361'


select count(*) from trips_stage2;
select * from trips_stage2;

SET datestyle = 'ISO, MDY';

COPY trips_stage2 (trip_id , trip_start_timestamp , trip_end_timestamp , 
			   trip_seconds ,trip_miles , percent_time_chicago , percent_distance_chicago , 
			   pickup_census_tract , dropoff_census_tract ,pickup_community_area , 
			   dropoff_community_area , fare , tip , additional_charges , 
			   trip_total ,shared_trip_authorized , shared_trip_match , 
			   trips_pooled , pickup_centroid_latitude , 
			   pickup_centroid_longitude , pickup_centroid_location , 
			   dropoff_centroid_latitude , dropoff_centroid_longitude ,
			   dropoff_centroid_location) 
FROM '/home/hp/Desktop/chintu/tran/top_100.csv'
WITH (FORMAT CSV, DELIMITER ',', HEADER);

-- UPDATE trips_stage2 SET trip_start_timestamp = TO_TIMESTAMP(trip_start_timestamp, 'MM/DD/YYYY HH:MI:SS AM');


select * from trips_dim2 where trip_id = '0000f4a746d9497e99324483a885b6bb50372361';
select * from trips_final2;

select count(*) from trips_dim2

truncate table trips_stage2;
truncate table trips_dim2;
truncate table trips_final2;

drop table trips_stage2;
drop table trips_dim2;
drop table trips_final2;
