import requests
import csv
import psycopg2
import traceback
from datetime import datetime


class Load_CSVData:
    global conn,cur
    print("Establishing Connection")
    conn = psycopg2.connect(dbname='amandb', user='aman', password=12345, host='localhost', port=5432)
    cur = conn.cursor()
    print("Connection established")
    
    def __init__(self,dbname, user, password, host, port):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port

    def get_load_data_csv(self,filepath):                                     
        try:
            # date_string = '01/06/2023 09:00:00 AM'
            # formatted_date = datetime.strptime(date_string, '%m/%d/%Y %I:%M:%S %p').strftime('%Y-%m-%d %H:%M:%S')

            cur.execute(f"""
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
               FROM '{filepath}'
               WITH (FORMAT CSV, DELIMITER ',', HEADER);
            """) 

            conn.commit()
            self.insert_trip_dim()
        except Exception as err:
            print(err)
            traceback.print_exc()
            conn.rollback()

    def insert_trip_dim(self):
        try:
            print("inserting into trips_dim2")
            cur.execute("""
            INSERT INTO trips_dim2 (created, updated, is_active, is_deleted,trip_id, trip_start_timestamp, trip_end_timestamp, trip_seconds,trip_miles, percent_time_chicago, percent_distance_chicago, pickup_census_tract, dropoff_census_tract,pickup_community_area, dropoff_community_area, fare, tip, additional_charges, trip_total,shared_trip_authorized, shared_trip_match, trips_pooled, pickup_centroid_latitude ,pickup_centroid_longitude, pickup_centroid_location, dropoff_centroid_latitude, dropoff_centroid_longitude,dropoff_centroid_location) 
            SELECT now(), now(), 1, 0, s.trip_id, s.trip_start_timestamp, s.trip_end_timestamp, s.trip_seconds ,s.trip_miles, s.percent_time_chicago, s.percent_distance_chicago, s.pickup_census_tract, s.dropoff_census_tract,s.pickup_community_area, s.dropoff_community_area, s.fare, s.tip, s.additional_charges, s.trip_total,s.shared_trip_authorized, s.shared_trip_match, s.trips_pooled, s.pickup_centroid_latitude ,s.pickup_centroid_longitude, s.pickup_centroid_location, s.dropoff_centroid_latitude, s.dropoff_centroid_longitude,s.dropoff_centroid_location
            FROM trips_stage2 s
            LEFT JOIN trips_dim2 d ON s.trip_id = d.trip_id
            WHERE d.trip_id IS NULL;
            """)

            print("inserting updated with same id into trips_dim2")
            cur.execute(""" INSERT INTO trips_dim2 (created, updated, is_active, is_deleted,trip_id, trip_start_timestamp, trip_end_timestamp, trip_seconds,trip_miles, percent_time_chicago, percent_distance_chicago, pickup_census_tract, dropoff_census_tract,pickup_community_area, dropoff_community_area, fare, tip, additional_charges, trip_total,shared_trip_authorized, shared_trip_match, trips_pooled, pickup_centroid_latitude ,pickup_centroid_longitude, pickup_centroid_location, dropoff_centroid_latitude, dropoff_centroid_longitude,dropoff_centroid_location)
            SELECT now(), now(), 1, 0, s.trip_id, s.trip_start_timestamp, s.trip_end_timestamp, s.trip_seconds ,s.trip_miles, s.percent_time_chicago, s.percent_distance_chicago, s.pickup_census_tract, s.dropoff_census_tract,s.pickup_community_area, s.dropoff_community_area, s.fare, s.tip, s.additional_charges, s.trip_total,s.shared_trip_authorized, s.shared_trip_match, s.trips_pooled, s.pickup_centroid_latitude ,s.pickup_centroid_longitude, s.pickup_centroid_location, s.dropoff_centroid_latitude, s.dropoff_centroid_longitude,s.dropoff_centroid_location
            FROM trips_stage2 s
            LEFT JOIN trips_dim2 t
            ON t.trip_id = s.trip_id
            WHERE t.trip_id = s.trip_id AND 
            (s.trip_start_timestamp<>t.trip_start_timestamp OR s.trip_end_timestamp<>t.trip_end_timestamp OR s.trip_seconds<>t.trip_seconds OR s.trip_miles<>t.trip_miles OR s.percent_time_chicago<>t.percent_time_chicago OR s.percent_distance_chicago<>t.percent_distance_chicago OR s.pickup_census_tract<>t.pickup_census_tract OR s.dropoff_census_tract<>t.dropoff_census_tract OR s.pickup_community_area<>t.pickup_community_area OR s.dropoff_community_area<>t.dropoff_community_area OR s.fare<>t.fare OR s.tip<>t.tip OR s.additional_charges<>t.additional_charges OR s.trip_total<>t.trip_total OR s.shared_trip_authorized<>t.shared_trip_authorized OR s.shared_trip_match<>t.shared_trip_match OR s.trips_pooled<>t.trips_pooled OR s.pickup_centroid_latitude<>t.pickup_centroid_latitude OR s.pickup_centroid_longitude<>t.pickup_centroid_longitude OR s.pickup_centroid_location<>t.pickup_centroid_location OR s.dropoff_centroid_latitude<>t.dropoff_centroid_latitude OR s.dropoff_centroid_longitude<>t.dropoff_centroid_longitude OR s.dropoff_centroid_location<>t.dropoff_centroid_location) 
            AND t.updated >= (SELECT MAX(created) FROM trips_dim2 WHERE t.trip_id = trips_dim2.trip_id) and is_active = 1 ;
            """)

            print("updating is_active and now() in trips_dim2")
            cur.execute(""" UPDATE trips_dim2 
            SET is_active = 0, updated = now()
            FROM trips_stage2 s
            WHERE s.trip_id = trips_dim2.trip_id AND (s.trip_start_timestamp<>trips_dim2.trip_start_timestamp OR s.trip_end_timestamp<>trips_dim2.trip_end_timestamp OR s.trip_seconds<>trips_dim2.trip_seconds OR s.trip_miles<>trips_dim2.trip_miles OR s.percent_time_chicago<>trips_dim2.percent_time_chicago OR s.percent_distance_chicago<>trips_dim2.percent_distance_chicago OR s.pickup_census_tract<>trips_dim2.pickup_census_tract OR s.dropoff_census_tract<>trips_dim2.dropoff_census_tract OR s.pickup_community_area<>trips_dim2.pickup_community_area OR s.dropoff_community_area<>trips_dim2.dropoff_community_area OR s.fare<>trips_dim2.fare OR s.tip<>trips_dim2.tip OR s.additional_charges<>trips_dim2.additional_charges OR s.trip_total<>trips_dim2.trip_total OR s.shared_trip_authorized<>trips_dim2.shared_trip_authorized OR s.shared_trip_match<>trips_dim2.shared_trip_match OR s.trips_pooled<>trips_dim2.trips_pooled OR s.pickup_centroid_latitude<>trips_dim2.pickup_centroid_latitude OR s.pickup_centroid_longitude<>trips_dim2.pickup_centroid_longitude OR s.pickup_centroid_location<>trips_dim2.pickup_centroid_location OR s.dropoff_centroid_latitude<>trips_dim2.dropoff_centroid_latitude OR s.dropoff_centroid_longitude<>trips_dim2.dropoff_centroid_longitude OR s.dropoff_centroid_location<>trips_dim2.dropoff_centroid_location) 
            AND is_active = 1;
            """)


            print("Updating is_active and is_deleted for dim")
            cur.execute("""
            UPDATE trips_dim2
            SET is_active = 0, is_deleted = 1, updated = now()
            WHERE NOT EXISTS (SELECT 1 FROM trips_stage2 WHERE trips_stage2.trip_id = trips_dim2.trip_id);
            """)
            conn.commit()
            self.insert_trip_final()
        except Exception as err:
            print('errrrrrrrrrrrrrrrrrrrrrr',err)
            traceback.print_exc()
            conn.rollback()

    def insert_trip_final(self):
        try:
            print(" Inserting new record into final from staging ")
            cur.execute(""" INSERT INTO trips_final2 (created, updated, is_active, is_deleted,trip_id, trip_start_timestamp, trip_end_timestamp, trip_seconds,trip_miles, percent_time_chicago, percent_distance_chicago, pickup_census_tract, dropoff_census_tract,pickup_community_area, dropoff_community_area, fare, tip, additional_charges, trip_total,shared_trip_authorized, shared_trip_match, trips_pooled, pickup_centroid_latitude ,pickup_centroid_longitude, pickup_centroid_location, dropoff_centroid_latitude, dropoff_centroid_longitude,dropoff_centroid_location)
            SELECT now(), now(), 1, 0, s.trip_id, s.trip_start_timestamp, s.trip_end_timestamp, s.trip_seconds ,s.trip_miles, s.percent_time_chicago, s.percent_distance_chicago, s.pickup_census_tract, s.dropoff_census_tract,s.pickup_community_area, s.dropoff_community_area, s.fare, s.tip, s.additional_charges, s.trip_total,s.shared_trip_authorized, s.shared_trip_match, s.trips_pooled, s.pickup_centroid_latitude ,s.pickup_centroid_longitude, s.pickup_centroid_location, s.dropoff_centroid_latitude, s.dropoff_centroid_longitude,s.dropoff_centroid_location  
            FROM trips_stage2 s
            LEFT JOIN trips_final2 f 
            on s.trip_id = f.trip_id
            WHERE f.trip_id IS NULL;
            """)

            print("updating values in trips_final2")
            cur.execute(""" UPDATE trips_final2 
            SET updated = now(), trip_start_timestamp=s.trip_start_timestamp, trip_end_timestamp=s.trip_end_timestamp, trip_seconds=s.trip_seconds ,trip_miles=s.trip_miles, percent_time_chicago=s.percent_time_chicago, percent_distance_chicago=s.percent_distance_chicago, pickup_census_tract=s.pickup_census_tract, dropoff_census_tract=s.dropoff_census_tract,pickup_community_area=s.pickup_community_area, dropoff_community_area=s.dropoff_community_area, fare=s.fare, tip=s.tip, additional_charges=s.additional_charges, trip_total=s.trip_total,shared_trip_authorized=s.shared_trip_authorized, shared_trip_match=s.shared_trip_match, trips_pooled=s.trips_pooled, pickup_centroid_latitude=s.pickup_centroid_latitude, pickup_centroid_longitude=s.pickup_centroid_longitude, pickup_centroid_location=s.pickup_centroid_location, dropoff_centroid_latitude=s.dropoff_centroid_latitude, dropoff_centroid_longitude=s.dropoff_centroid_longitude, dropoff_centroid_location=s.dropoff_centroid_location, is_active =1 
            FROM trips_stage2 s
            WHERE s.trip_id = trips_final2.trip_id AND (s.trip_start_timestamp<>trips_final2.trip_start_timestamp OR s.trip_end_timestamp<>trips_final2.trip_end_timestamp OR s.trip_seconds<>trips_final2.trip_seconds OR s.trip_miles<>trips_final2.trip_miles OR s.percent_time_chicago<>trips_final2.percent_time_chicago OR s.percent_distance_chicago<>trips_final2.percent_distance_chicago OR s.pickup_census_tract<>trips_final2.pickup_census_tract OR s.dropoff_census_tract<>trips_final2.dropoff_census_tract OR s.pickup_community_area<>trips_final2.pickup_community_area OR s.dropoff_community_area<>trips_final2.dropoff_community_area OR s.fare<>trips_final2.fare OR s.tip<>trips_final2.tip OR s.additional_charges<>trips_final2.additional_charges OR s.trip_total<>trips_final2.trip_total OR s.shared_trip_authorized<>trips_final2.shared_trip_authorized OR s.shared_trip_match<>trips_final2.shared_trip_match OR s.trips_pooled<>trips_final2.trips_pooled OR s.pickup_centroid_latitude<>trips_final2.pickup_centroid_latitude OR s.pickup_centroid_longitude<>trips_final2.pickup_centroid_longitude OR s.pickup_centroid_location<>trips_final2.pickup_centroid_location OR s.dropoff_centroid_latitude<>trips_final2.dropoff_centroid_latitude OR s.dropoff_centroid_longitude<>trips_final2.dropoff_centroid_longitude OR s.dropoff_centroid_location<>trips_final2.dropoff_centroid_location);
            """)

            print("updating is_active , is_deleted and updated in trips_final2")            
            cur.execute(""" UPDATE trips_final2 
            SET is_active = 0, is_deleted = 1, updated = now()
            WHERE NOT EXISTS (SELECT 1 FROM trips_stage2 WHERE trips_final2.trip_id = trips_stage2.trip_id);
            """)

            cur.execute("""TRUNCATE TABLE trips_stage2""")
            conn.commit()
        except Exception as err:
            print('errrrrrrrrrrrrrrrrrrrrrr',err)
            traceback.print_exc()
            conn.rollback()
        finally:
            cur.close()
            conn.close()

my_obj = Load_CSVData('amandb', 'aman', 12345, 'localhost', 5432)
file_path = '/home/hp/Desktop/chintu/tran/top_100.csv'
my_obj.get_load_data_csv(file_path)

# for i in range(15):
#     file_path = '/home/hp/Downloads/trips_{:02d}.csv'.format(i)
#     my_obj.get_load_data_csv(file_path) 



#######################################################################################
# CORRECT_COMMAND : split -l 50001 -d --filter='cat > $FILE.csv' Transportation_Network_Providers_Trips_2023.csv trips_

# 747720    -----------1f09b3ee2a57e7ede5e33f099cbe34dbe9926490	01/29/2023 12:45:00 PM	01/29/2023 12:45:00 PM	177	1	1	1			41	41	12.5	0	1.26	13.76	false	false	1	41.79{

# head -n 100 Transportation_Network_Providers_Trips_2023.csv > top_100.csv

# ?$where=%trip_start_timestamp%20between%20%272023-02-28T00:00:00%27%20and%20%272023-03-31T00:00:00%27

#  https://data.cityofchicago.org/Transportation/Transportation-Network-Providers-Trips-2023-/ukxa-jdd7
#################  https://data.cityofchicago.org/resource/ukxa-jdd7.json

# https://data.cityofchicago.org/Transportation/Transportation-Network-Provider-Trips-by-Month-202/krwv-nnih
################# https://data.cityofchicago.org/resource/n26f-ihde.json