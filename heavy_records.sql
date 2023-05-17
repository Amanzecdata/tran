CREATE TABLE trips_stagee(trip_id Text, 
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
			   
CREATE TABLE trips_dimm( created timestamp default current_timestamp,
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

CREATE TABLE trips_finall( created timestamp default current_timestamp,
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
			   
ANALYZE INSERT INTO trips_dim1 (created, updated, is_active, is_deleted,trip_id, trip_start_timestamp, trip_end_timestamp, trip_seconds,trip_miles, percent_time_chicago, percent_distance_chicago, pickup_census_tract, dropoff_census_tract,pickup_community_area, dropoff_community_area, fare, tip, additional_charges, trip_total,shared_trip_authorized, shared_trip_match, trips_pooled, pickup_centroid_latitude ,pickup_centroid_longitude, pickup_centroid_location, dropoff_centroid_latitude, dropoff_centroid_longitude,dropoff_centroid_location)
            SELECT now(), now(), 1, 0, s.trip_id, s.trip_start_timestamp, s.trip_end_timestamp, s.trip_seconds ,s.trip_miles, s.percent_time_chicago, s.percent_distance_chicago, s.pickup_census_tract, s.dropoff_census_tract,s.pickup_community_area, s.dropoff_community_area, s.fare, s.tip, s.additional_charges, s.trip_total,s.shared_trip_authorized, s.shared_trip_match, s.trips_pooled, s.pickup_centroid_latitude ,s.pickup_centroid_longitude, s.pickup_centroid_location, s.dropoff_centroid_latitude, s.dropoff_centroid_longitude,s.dropoff_centroid_location
            FROM trips_stage2 s
            LEFT JOIN trips_dim1 t
            ON t.trip_id = s.trip_id
            WHERE t.trip_id = s.trip_id AND 
            (s.trip_start_timestamp<>t.trip_start_timestamp OR s.trip_end_timestamp<>t.trip_end_timestamp OR s.trip_seconds<>t.trip_seconds OR s.trip_miles<>t.trip_miles OR s.percent_time_chicago<>t.percent_time_chicago OR s.percent_distance_chicago<>t.percent_distance_chicago OR s.pickup_census_tract<>t.pickup_census_tract OR s.dropoff_census_tract<>t.dropoff_census_tract OR s.pickup_community_area<>t.pickup_community_area OR s.dropoff_community_area<>t.dropoff_community_area OR s.fare<>t.fare OR s.tip<>t.tip OR s.additional_charges<>t.additional_charges OR s.trip_total<>t.trip_total OR s.shared_trip_authorized<>t.shared_trip_authorized OR s.shared_trip_match<>t.shared_trip_match OR s.trips_pooled<>t.trips_pooled OR s.pickup_centroid_latitude<>t.pickup_centroid_latitude OR s.pickup_centroid_longitude<>t.pickup_centroid_longitude OR s.pickup_centroid_location<>t.pickup_centroid_location OR s.dropoff_centroid_latitude<>t.dropoff_centroid_latitude OR s.dropoff_centroid_longitude<>t.dropoff_centroid_longitude OR s.dropoff_centroid_location<>t.dropoff_centroid_location) 
            AND t.updated >= (SELECT MAX(created) FROM trips_dim1 WHERE t.trip_id = trips_dim1.trip_id) and is_active = 1 ;


select * from trips_stagee;
select * from trips_dimm;
select * from trips_finall;

select count(*) from trips_stagee;
select count(*) from trips_dimm;
select count(*) from trips_finall;


drop table trips_stagee
truncate table trips_stagee;
truncate table trips_dimm;
truncate table trips_finall;