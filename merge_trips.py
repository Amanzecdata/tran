import psycopg2
import time
import traceback
conn = psycopg2.connect(
    host="localhost",
    database="amandb",
    user="aman",
    password="12345"
)
cur = conn.cursor()
print("connection established")

def merge_trips():
    try:
        print("Inserting unique records")
        cur.execute("""
            INSERT INTO merge_trips(trip_id , trip_start_timestamp , trip_end_timestamp , 
			   trip_seconds ,trip_miles , percent_time_chicago , percent_distance_chicago , 
			   pickup_census_tract , dropoff_census_tract ,pickup_community_area , 
			   dropoff_community_area , fare , tip , additional_charges , 
			   trip_total ,shared_trip_authorized , shared_trip_match , 
			   trips_pooled , pickup_centroid_latitude , 
			   pickup_centroid_longitude , pickup_centroid_location , 
			   dropoff_centroid_latitude , dropoff_centroid_longitude ,
			   dropoff_centroid_location, created, updated, is_active, is_deleted)
    SELECT COALESCE(f.trip_id, d.trip_id) AS trip_id, 
           COALESCE(f.trip_start_timestamp,d.trip_start_timestamp), 
           COALESCE(f.trip_end_timestamp,d.trip_end_timestamp),
           COALESCE(f.trip_seconds,d.trip_seconds), 
           COALESCE(f.trip_miles,d.trip_miles),
           COALESCE(f.percent_time_chicago,d.percent_time_chicago),
           COALESCE(f.percent_distance_chicago,d.percent_distance_chicago),
           COALESCE(f.pickup_census_tract,d.pickup_census_tract),
           COALESCE(f.dropoff_census_tract,d.dropoff_census_tract),
           COALESCE(f.pickup_community_area,d.pickup_community_area),
           COALESCE(f.dropoff_community_area,d.dropoff_community_area),
           COALESCE(f.fare,d.fare),
           COALESCE(f.tip,d.tip),
            COALESCE(f.additional_charges,d.additional_charges),
            COALESCE(f.trip_total,d.trip_total),
            COALESCE(f.shared_trip_authorized,d.shared_trip_authorized),
            COALESCE(f.shared_trip_match,d.shared_trip_match),
            COALESCE(f.trips_pooled,d.trips_pooled),
            COALESCE(f.pickup_centroid_latitude,d.pickup_centroid_latitude),
            COALESCE(f.pickup_centroid_longitude,d.pickup_centroid_longitude),
            COALESCE(f.pickup_centroid_location,d.pickup_centroid_location), 
            COALESCE(f.dropoff_centroid_latitude,d.dropoff_centroid_latitude), 
            COALESCE(f.dropoff_centroid_longitude,d.dropoff_centroid_longitude), 
            COALESCE(f.dropoff_centroid_location,d.dropoff_centroid_location),
            now(), now(),
    CASE
        WHEN f.is_active = 1 AND d.is_active = 1 THEN 1
		WHEN f.is_active =0 AND d.is_active = 0 THEN 0
        ELSE 1
    END AS is_active,
    CASE
        WHEN f.is_deleted = 1 AND d.is_deleted = 1 THEN 1
		WHEN f.is_deleted = 0 AND d.is_deleted = 0 THEN 0
        ELSE 0
    END AS is_deleted
    FROM trips_final1 AS f
    FULL JOIN trips_final2 AS d ON f.trip_id = d.trip_id
    WHERE COALESCE(f.trip_id, d.trip_id) NOT IN (SELECT trip_id FROM merge_trips);""")
        
        print("updating records")
        cur.execute("""UPDATE merge_trips m
        SET trip_start_timestamp = COALESCE(f.trip_start_timestamp,d.trip_start_timestamp), 
           trip_end_timestamp = COALESCE(f.trip_end_timestamp,d.trip_end_timestamp),
           trip_seconds = COALESCE(f.trip_seconds,d.trip_seconds), 
           trip_miles = COALESCE(f.trip_miles,d.trip_miles),
           percent_time_chicago = COALESCE(f.percent_time_chicago,d.percent_time_chicago),
           percent_distance_chicago = COALESCE(f.percent_distance_chicago,d.percent_distance_chicago),
           pickup_census_tract = COALESCE(f.pickup_census_tract,d.pickup_census_tract),
           dropoff_census_tract = COALESCE(f.dropoff_census_tract,d.dropoff_census_tract),
           pickup_community_area = COALESCE(f.pickup_community_area,d.pickup_community_area),
           dropoff_community_area = COALESCE(f.dropoff_community_area,d.dropoff_community_area),
           fare = COALESCE(f.fare,d.fare),
           tip = COALESCE(f.tip,d.tip),
            additional_charges = COALESCE(f.additional_charges,d.additional_charges),
            trip_total = COALESCE(f.trip_total,d.trip_total),
            shared_trip_authorized = COALESCE(f.shared_trip_authorized,d.shared_trip_authorized),
            shared_trip_match = COALESCE(f.shared_trip_match,d.shared_trip_match),
            trips_pooled = COALESCE(f.trips_pooled,d.trips_pooled),
            pickup_centroid_latitude = COALESCE(f.pickup_centroid_latitude,d.pickup_centroid_latitude),
            pickup_centroid_longitude = COALESCE(f.pickup_centroid_longitude,d.pickup_centroid_longitude),
            pickup_centroid_location = COALESCE(f.pickup_centroid_location,d.pickup_centroid_location), 
            dropoff_centroid_latitude = COALESCE(f.dropoff_centroid_latitude,d.dropoff_centroid_latitude), 
            dropoff_centroid_longitude = COALESCE(f.dropoff_centroid_longitude,d.dropoff_centroid_longitude), 
            dropoff_centroid_location = COALESCE(f.dropoff_centroid_location,d.dropoff_centroid_location),
            updated = now(),
        is_active = CASE
            WHEN f.is_active = 1 AND d.is_active = 1 THEN 1
		    WHEN f.is_active =0 AND d.is_active = 0 THEN 0
            ELSE 1
        END,
        is_deleted = CASE
            WHEN f.is_deleted = 1 AND d.is_deleted = 1 THEN 1
		    WHEN f.is_deleted = 0 AND d.is_deleted = 0 THEN 0
            ELSE 0
        END
            FROM trips_final1 AS f
            FULL JOIN trips_final2 AS d ON f.trip_id = d.trip_id
            WHERE COALESCE(f.trip_id, d.trip_id) = m.trip_id AND
            (m.trip_start_timestamp<>COALESCE(d.trip_start_timestamp,f.trip_start_timestamp) 
            OR m.trip_end_timestamp<>COALESCE(d.trip_end_timestamp,f.trip_end_timestamp) 
            OR m.trip_seconds<>COALESCE(d.trip_seconds,f.trip_seconds) 
            OR m.trip_miles<>COALESCE(d.trip_miles,f.trip_miles) 
            OR m.percent_time_chicago<>COALESCE(d.percent_time_chicago,f.percent_time_chicago) 
            OR m.percent_distance_chicago<>COALESCE(d.percent_distance_chicago,f.percent_distance_chicago) 
            OR m.pickup_census_tract<>COALESCE(d.pickup_census_tract,f.pickup_census_tract) 
            OR m.dropoff_census_tract<>COALESCE(d.dropoff_census_tract,f.dropoff_census_tract) 
            OR m.pickup_community_area<>COALESCE(d.pickup_community_area,f.pickup_community_area) 
            OR m.dropoff_community_area<>COALESCE(d.dropoff_community_area,f.dropoff_community_area) 
            OR m.fare<>COALESCE(d.fare,f.fare) 
            OR m.tip<>COALESCE(d.tip,f.tip) 
            OR m.additional_charges<>COALESCE(d.additional_charges,f.additional_charges) 
            OR m.trip_total<>COALESCE(d.trip_total,f.trip_total) 
            OR m.shared_trip_authorized<>COALESCE(d.shared_trip_authorized,f.shared_trip_authorized) 
            OR m.shared_trip_match<>COALESCE(d.shared_trip_match,f.shared_trip_match) 
            OR m.trips_pooled<>COALESCE(d.trips_pooled,f.trips_pooled) 
            OR m.pickup_centroid_latitude<>COALESCE(d.pickup_centroid_latitude,f.pickup_centroid_latitude) 
            OR m.pickup_centroid_longitude<>COALESCE(d.pickup_centroid_longitude,f.pickup_centroid_longitude) 
            OR m.pickup_centroid_location<>COALESCE(d.pickup_centroid_location,f.pickup_centroid_location) 
            OR m.dropoff_centroid_latitude<>COALESCE(d.dropoff_centroid_latitude,f.dropoff_centroid_latitude) 
            OR m.dropoff_centroid_longitude<>COALESCE(d.dropoff_centroid_longitude,f.dropoff_centroid_longitude) 
            OR m.dropoff_centroid_location<>COALESCE(d.dropoff_centroid_location,f.dropoff_centroid_location))
            ;""")
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(e)
        conn.commit()
        conn.close()
        return e
print(merge_trips())