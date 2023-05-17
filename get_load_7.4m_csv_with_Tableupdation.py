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
    
    def __init__(self,table1,table2,tabl3):
        self.table1 = table1
        self.table2 = table2
        self.table3 = table3

    def get_load_data_csv(self,filepath):                                     
        try:
            cur.execute(f"""
               SET datestyle = 'ISO, MDY';
               COPY {self.table1} FROM '{filepath}' DELIMITER ',' CSV HEADER;""") 
            conn.commit()
            self.insert_trip_dim()
        except Exception as err:
            print(err)
            traceback.print_exc()
            conn.rollback()

    def insert_trip_dim(self):
        try:
            print(f"inserting into {self.table2}")
            cur.execute(f"""
            INSERT INTO {self.table2} (created, updated, is_active, is_deleted,trip_id, trip_start_timestamp, trip_end_timestamp, trip_seconds,trip_miles, percent_time_chicago, percent_distance_chicago, pickup_census_tract, dropoff_census_tract,pickup_community_area, dropoff_community_area, fare, tip, additional_charges, trip_total,shared_trip_authorized, shared_trip_match, trips_pooled, pickup_centroid_latitude ,pickup_centroid_longitude, pickup_centroid_location, dropoff_centroid_latitude, dropoff_centroid_longitude,dropoff_centroid_location) 
            SELECT now(), now(), 1, 0, s.trip_id, s.trip_start_timestamp, s.trip_end_timestamp, s.trip_seconds ,s.trip_miles, s.percent_time_chicago, s.percent_distance_chicago, s.pickup_census_tract, s.dropoff_census_tract,s.pickup_community_area, s.dropoff_community_area, s.fare, s.tip, s.additional_charges, s.trip_total,s.shared_trip_authorized, s.shared_trip_match, s.trips_pooled, s.pickup_centroid_latitude ,s.pickup_centroid_longitude, s.pickup_centroid_location, s.dropoff_centroid_latitude, s.dropoff_centroid_longitude,s.dropoff_centroid_location
            FROM {self.table1} s
            LEFT JOIN {self.table2} d ON s.trip_id = d.trip_id
            WHERE d.trip_id IS NULL;
            """)

            print(f"inserting updated with same id into {self.table2}")
            cur.execute(f""" INSERT INTO {self.table2} (created, updated, is_active, is_deleted,trip_id, trip_start_timestamp, trip_end_timestamp, trip_seconds,trip_miles, percent_time_chicago, percent_distance_chicago, pickup_census_tract, dropoff_census_tract,pickup_community_area, dropoff_community_area, fare, tip, additional_charges, trip_total,shared_trip_authorized, shared_trip_match, trips_pooled, pickup_centroid_latitude ,pickup_centroid_longitude, pickup_centroid_location, dropoff_centroid_latitude, dropoff_centroid_longitude,dropoff_centroid_location)
            SELECT now(), now(), 1, 0, s.trip_id, s.trip_start_timestamp, s.trip_end_timestamp, s.trip_seconds ,s.trip_miles, s.percent_time_chicago, s.percent_distance_chicago, s.pickup_census_tract, s.dropoff_census_tract,s.pickup_community_area, s.dropoff_community_area, s.fare, s.tip, s.additional_charges, s.trip_total,s.shared_trip_authorized, s.shared_trip_match, s.trips_pooled, s.pickup_centroid_latitude ,s.pickup_centroid_longitude, s.pickup_centroid_location, s.dropoff_centroid_latitude, s.dropoff_centroid_longitude,s.dropoff_centroid_location
            FROM {self.table1} s
            LEFT JOIN {self.table2} t
            ON t.trip_id = s.trip_id
            WHERE t.trip_id = s.trip_id AND 
            (s.trip_start_timestamp<>t.trip_start_timestamp OR s.trip_end_timestamp<>t.trip_end_timestamp OR s.trip_seconds<>t.trip_seconds OR s.trip_miles<>t.trip_miles OR s.percent_time_chicago<>t.percent_time_chicago OR s.percent_distance_chicago<>t.percent_distance_chicago OR s.pickup_census_tract<>t.pickup_census_tract OR s.dropoff_census_tract<>t.dropoff_census_tract OR s.pickup_community_area<>t.pickup_community_area OR s.dropoff_community_area<>t.dropoff_community_area OR s.fare<>t.fare OR s.tip<>t.tip OR s.additional_charges<>t.additional_charges OR s.trip_total<>t.trip_total OR s.shared_trip_authorized<>t.shared_trip_authorized OR s.shared_trip_match<>t.shared_trip_match OR s.trips_pooled<>t.trips_pooled OR s.pickup_centroid_latitude<>t.pickup_centroid_latitude OR s.pickup_centroid_longitude<>t.pickup_centroid_longitude OR s.pickup_centroid_location<>t.pickup_centroid_location OR s.dropoff_centroid_latitude<>t.dropoff_centroid_latitude OR s.dropoff_centroid_longitude<>t.dropoff_centroid_longitude OR s.dropoff_centroid_location<>t.dropoff_centroid_location) 
            AND t.updated >= (SELECT MAX(created) FROM {self.table2} WHERE t.trip_id = {self.table2}.trip_id) and is_active = 1 ;
            """)

            print(f"updating is_active and now() in {self.table2}")
            cur.execute(f""" UPDATE {self.table2} 
            SET is_active = 0, updated = now()
            FROM {self.table1} s
            WHERE s.trip_id = {self.table2}.trip_id AND (s.trip_start_timestamp<>{self.table2}.trip_start_timestamp OR s.trip_end_timestamp<>{self.table2}.trip_end_timestamp OR s.trip_seconds<>{self.table2}.trip_seconds OR s.trip_miles<>{self.table2}.trip_miles OR s.percent_time_chicago<>{self.table2}.percent_time_chicago OR s.percent_distance_chicago<>{self.table2}.percent_distance_chicago OR s.pickup_census_tract<>{self.table2}.pickup_census_tract OR s.dropoff_census_tract<>{self.table2}.dropoff_census_tract OR s.pickup_community_area<>{self.table2}.pickup_community_area OR s.dropoff_community_area<>{self.table2}.dropoff_community_area OR s.fare<>{self.table2}.fare OR s.tip<>{self.table2}.tip OR s.additional_charges<>{self.table2}.additional_charges OR s.trip_total<>{self.table2}.trip_total OR s.shared_trip_authorized<>{self.table2}.shared_trip_authorized OR s.shared_trip_match<>{self.table2}.shared_trip_match OR s.trips_pooled<>{self.table2}.trips_pooled OR s.pickup_centroid_latitude<>{self.table2}.pickup_centroid_latitude OR s.pickup_centroid_longitude<>{self.table2}.pickup_centroid_longitude OR s.pickup_centroid_location<>{self.table2}.pickup_centroid_location OR s.dropoff_centroid_latitude<>{self.table2}.dropoff_centroid_latitude OR s.dropoff_centroid_longitude<>{self.table2}.dropoff_centroid_longitude OR s.dropoff_centroid_location<>{self.table2}.dropoff_centroid_location) 
            AND is_active = 1;
            """)


            print("Updating is_active and is_deleted for dim")
            cur.execute(f"""
            UPDATE {self.table2}
            SET is_active = 0, is_deleted = 1, updated = now()
            WHERE NOT EXISTS (SELECT 1 FROM {self.table1} WHERE {self.table1}.trip_id = {self.table2}.trip_id);
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
            cur.execute(f""" INSERT INTO {self.table3} (created, updated, is_active, is_deleted,trip_id, trip_start_timestamp, trip_end_timestamp, trip_seconds,trip_miles, percent_time_chicago, percent_distance_chicago, pickup_census_tract, dropoff_census_tract,pickup_community_area, dropoff_community_area, fare, tip, additional_charges, trip_total,shared_trip_authorized, shared_trip_match, trips_pooled, pickup_centroid_latitude ,pickup_centroid_longitude, pickup_centroid_location, dropoff_centroid_latitude, dropoff_centroid_longitude,dropoff_centroid_location)
            SELECT now(), now(), 1, 0, s.trip_id, s.trip_start_timestamp, s.trip_end_timestamp, s.trip_seconds ,s.trip_miles, s.percent_time_chicago, s.percent_distance_chicago, s.pickup_census_tract, s.dropoff_census_tract,s.pickup_community_area, s.dropoff_community_area, s.fare, s.tip, s.additional_charges, s.trip_total,s.shared_trip_authorized, s.shared_trip_match, s.trips_pooled, s.pickup_centroid_latitude ,s.pickup_centroid_longitude, s.pickup_centroid_location, s.dropoff_centroid_latitude, s.dropoff_centroid_longitude,s.dropoff_centroid_location  
            FROM {self.table1} AS s
            LEFT JOIN {self.table3} AS f 
            on s.trip_id = f.trip_id
            WHERE f.trip_id IS NULL AND 
            NOT EXISTS (SELECT 1 FROM {self.table3} WHERE {self.table3}.trip_id = s.trip_id);
            """)

            print(f"updating values in {self.table3}")
            cur.execute(f""" UPDATE {self.table3} 
            SET updated = now(), trip_start_timestamp=s.trip_start_timestamp, trip_end_timestamp=s.trip_end_timestamp, trip_seconds=s.trip_seconds ,trip_miles=s.trip_miles, percent_time_chicago=s.percent_time_chicago, percent_distance_chicago=s.percent_distance_chicago, pickup_census_tract=s.pickup_census_tract, dropoff_census_tract=s.dropoff_census_tract,pickup_community_area=s.pickup_community_area, dropoff_community_area=s.dropoff_community_area, fare=s.fare, tip=s.tip, additional_charges=s.additional_charges, trip_total=s.trip_total,shared_trip_authorized=s.shared_trip_authorized, shared_trip_match=s.shared_trip_match, trips_pooled=s.trips_pooled, pickup_centroid_latitude=s.pickup_centroid_latitude, pickup_centroid_longitude=s.pickup_centroid_longitude, pickup_centroid_location=s.pickup_centroid_location, dropoff_centroid_latitude=s.dropoff_centroid_latitude, dropoff_centroid_longitude=s.dropoff_centroid_longitude, dropoff_centroid_location=s.dropoff_centroid_location, is_active =1 
            FROM {self.table1} s
            WHERE s.trip_id = {self.table3}.trip_id AND (s.trip_start_timestamp<>{self.table3}.trip_start_timestamp OR s.trip_end_timestamp<>{self.table3}.trip_end_timestamp OR s.trip_seconds<>{self.table3}.trip_seconds OR s.trip_miles<>{self.table3}.trip_miles OR s.percent_time_chicago<>{self.table3}.percent_time_chicago OR s.percent_distance_chicago<>{self.table3}.percent_distance_chicago OR s.pickup_census_tract<>{self.table3}.pickup_census_tract OR s.dropoff_census_tract<>{self.table3}.dropoff_census_tract OR s.pickup_community_area<>{self.table3}.pickup_community_area OR s.dropoff_community_area<>{self.table3}.dropoff_community_area OR s.fare<>{self.table3}.fare OR s.tip<>{self.table3}.tip OR s.additional_charges<>{self.table3}.additional_charges OR s.trip_total<>{self.table3}.trip_total OR s.shared_trip_authorized<>{self.table3}.shared_trip_authorized OR s.shared_trip_match<>{self.table3}.shared_trip_match OR s.trips_pooled<>{self.table3}.trips_pooled OR s.pickup_centroid_latitude<>{self.table3}.pickup_centroid_latitude OR s.pickup_centroid_longitude<>{self.table3}.pickup_centroid_longitude OR s.pickup_centroid_location<>{self.table3}.pickup_centroid_location OR s.dropoff_centroid_latitude<>{self.table3}.dropoff_centroid_latitude OR s.dropoff_centroid_longitude<>{self.table3}.dropoff_centroid_longitude OR s.dropoff_centroid_location<>{self.table3}.dropoff_centroid_location);
            """)

            print(f"updating is_active , is_deleted and updated in {self.table3}")            
            cur.execute(f""" UPDATE {self.table3} 
            SET is_active = 0, is_deleted = 1, updated = now()
            WHERE NOT EXISTS (SELECT 1 FROM {self.table1} WHERE {self.table3}.trip_id = {self.table1}.trip_id);
            """)

            cur.execute(f"""TRUNCATE TABLE {self.table1}""")
            conn.commit()
        except Exception as err:
            print('errrrrrrrrrrrrrrrrrrrrrr',err)
            traceback.print_exc()
            conn.rollback()
        
table1 = 'trips_stagee'
table2 = 'trips_dimm'
table3 = 'trips_finall'
my_obj = Load_CSVData(table1,table2,table3 )

for i in range(15):
    file_path = '/home/hp/Desktop/chintu/tran/trips_{:02d}.csv'.format(i)
    my_obj.get_load_data_csv(file_path) 
    print("---------------------- uploaded file no. : ",i+1)
    if i>=15:
        print('closing conn')
        cur.close()
        conn.close()



#######################################################################################
# CORRECT_COMMAND : split -l 50001 -d --filter='cat > $FILE.csv' Transportation_Network_Providers_Trips_2023.csv trips_

# 747720    -----------1f09b3ee2a57e7ede5e33f099cbe34dbe9926490	01/29/2023 12:45:00 PM	01/29/2023 12:45:00 PM	177	1	1	1			41	41	12.5	0	1.26	13.76	false	false	1	41.79{

# head -n 100 Transportation_Network_Providers_Trips_2023.csv > top_100.csv

# ?$where=%trip_start_timestamp%20between%20%272023-02-28T00:00:00%27%20and%20%272023-03-31T00:00:00%27

#  https://data.cityofchicago.org/Transportation/Transportation-Network-Providers-Trips-2023-/ukxa-jdd7
#################  https://data.cityofchicago.org/resource/ukxa-jdd7.json

# https://data.cityofchicago.org/Transportation/Transportation-Network-Provider-Trips-by-Month-202/krwv-nnih
################# https://data.cityofchicago.org/resource/n26f-ihde.json

