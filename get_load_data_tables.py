import requests
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import json
import traceback
import csv

class Load_APIData:
    global conn,cur
    print("Establishing Connection")
    conn = psycopg2.connect(
        dbname='amandb',
        user='aman',
        password=12345,
        host='localhost',
        port=5432
    )
    cur = conn.cursor()
    print("Connection established")

    def __init__(self, url, dbprovider, dbname, user, password, host, port):
        self.url = url
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.dbprovider = dbprovider
        self.data = self.get_data()
        self.insert_trip_stage(self.data)

    def get_data(self):
        response = requests.get(self.url)
        print("Data get successfully")
        return response.json()

    def insert_trip_stage(self, data):
        try:
            # Insert data into table
            db_string = f"{self.dbprovider}://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"
            engine = create_engine(db_string)
            response = requests.get(self.url).json()
            df = pd.DataFrame(response)

            # Convert the pickup_centroid_location and dropoff_centroid_location columns to JSON strings
            df['pickup_centroid_location'] = df['pickup_centroid_location'].apply(json.dumps)
            df['dropoff_centroid_location'] = df['dropoff_centroid_location'].apply(json.dumps)
            df.to_sql(name='trips_stage', con=engine, if_exists='append', index=False)
            conn.commit()
            self.insert_trip_dim()
        except Exception as err:
            print("error : ",err)
            traceback.print_exc()
            conn.rollback()           
    def insert_trip_dim(self):
        try:
            cur.execute("""
            INSERT INTO trips_dim (created, updated, is_active, is_deleted,trip_id, trip_start_timestamp, trip_end_timestamp, trip_seconds,trip_miles, percent_time_chicago, percent_distance_chicago, pickup_census_tract, dropoff_census_tract,pickup_community_area, dropoff_community_area, fare, tip, additional_charges, trip_total,shared_trip_authorized, shared_trip_match, trips_pooled, pickup_centroid_latitude ,pickup_centroid_longitude, pickup_centroid_location, dropoff_centroid_latitude, dropoff_centroid_longitude,dropoff_centroid_location) 
            SELECT now(), now(), 1, 0, s.trip_id, s.trip_start_timestamp, s.trip_end_timestamp, s.trip_seconds ,s.trip_miles, s.percent_time_chicago, s.percent_distance_chicago, s.pickup_census_tract, s.dropoff_census_tract,s.pickup_community_area, s.dropoff_community_area, s.fare, s.tip, s.additional_charges, s.trip_total,s.shared_trip_authorized, s.shared_trip_match, s.trips_pooled, s.pickup_centroid_latitude ,s.pickup_centroid_longitude, s.pickup_centroid_location, s.dropoff_centroid_latitude, s.dropoff_centroid_longitude,s.dropoff_centroid_location
            FROM trips_stage s
            LEFT JOIN trips_dim d ON s.trip_id = d.trip_id
            WHERE d.trip_id IS NULL;
            """)

            cur.execute(""" INSERT INTO trips_dim (created, updated, is_active, is_deleted,trip_id, trip_start_timestamp, trip_end_timestamp, trip_seconds,trip_miles, percent_time_chicago, percent_distance_chicago, pickup_census_tract, dropoff_census_tract,pickup_community_area, dropoff_community_area, fare, tip, additional_charges, trip_total,shared_trip_authorized, shared_trip_match, trips_pooled, pickup_centroid_latitude ,pickup_centroid_longitude, pickup_centroid_location, dropoff_centroid_latitude, dropoff_centroid_longitude,dropoff_centroid_location)
            SELECT now(), now(), 1, 0, s.trip_id, s.trip_start_timestamp, s.trip_end_timestamp, s.trip_seconds ,s.trip_miles, s.percent_time_chicago, s.percent_distance_chicago, s.pickup_census_tract, s.dropoff_census_tract,s.pickup_community_area, s.dropoff_community_area, s.fare, s.tip, s.additional_charges, s.trip_total,s.shared_trip_authorized, s.shared_trip_match, s.trips_pooled, s.pickup_centroid_latitude ,s.pickup_centroid_longitude, s.pickup_centroid_location, s.dropoff_centroid_latitude, s.dropoff_centroid_longitude,s.dropoff_centroid_location
            FROM trips_stage s
            LEFT JOIN trips_dim t
            ON t.trip_id = s.trip_id
            WHERE t.trip_id = s.trip_id AND 
            (s.trip_start_timestamp<>t.trip_start_timestamp OR s.trip_end_timestamp<>t.trip_end_timestamp OR s.trip_seconds<>t.trip_seconds OR s.trip_miles<>t.trip_miles OR s.percent_time_chicago<>t.percent_time_chicago OR s.percent_distance_chicago<>t.percent_distance_chicago OR s.pickup_census_tract<>t.pickup_census_tract OR s.dropoff_census_tract<>t.dropoff_census_tract OR s.pickup_community_area<>t.pickup_community_area OR s.dropoff_community_area<>t.dropoff_community_area OR s.fare<>t.fare OR s.tip<>t.tip OR s.additional_charges<>t.additional_charges OR s.trip_total<>t.trip_total OR s.shared_trip_authorized<>t.shared_trip_authorized OR s.shared_trip_match<>t.shared_trip_match OR s.trips_pooled<>t.trips_pooled OR s.pickup_centroid_latitude<>t.pickup_centroid_latitude OR s.pickup_centroid_longitude<>t.pickup_centroid_longitude OR s.pickup_centroid_location<>t.pickup_centroid_location OR s.dropoff_centroid_latitude<>t.dropoff_centroid_latitude OR s.dropoff_centroid_longitude<>t.dropoff_centroid_longitude OR s.dropoff_centroid_location<>t.dropoff_centroid_location) 
            AND t.updated > (SELECT MAX(created) FROM trips_dim WHERE t.trip_id = trips_dim.trip_id);
            """)

            cur.execute(""" UPDATE trips_dim 
            SET is_active = 0, updated = now()
            FROM trips_stage s
            WHERE s.trip_id = trips_dim.trip_id AND (s.trip_start_timestamp<>trips_dim.trip_start_timestamp OR s.trip_end_timestamp<>trips_dim.trip_end_timestamp OR s.trip_seconds<>trips_dim.trip_seconds OR s.trip_miles<>trips_dim.trip_miles OR s.percent_time_chicago<>trips_dim.percent_time_chicago OR s.percent_distance_chicago<>trips_dim.percent_distance_chicago OR s.pickup_census_tract<>trips_dim.pickup_census_tract OR s.dropoff_census_tract<>trips_dim.dropoff_census_tract OR s.pickup_community_area<>trips_dim.pickup_community_area OR s.dropoff_community_area<>trips_dim.dropoff_community_area OR s.fare<>trips_dim.fare OR s.tip<>trips_dim.tip OR s.additional_charges<>trips_dim.additional_charges OR s.trip_total<>trips_dim.trip_total OR s.shared_trip_authorized<>trips_dim.shared_trip_authorized OR s.shared_trip_match<>trips_dim.shared_trip_match OR s.trips_pooled<>trips_dim.trips_pooled OR s.pickup_centroid_latitude<>trips_dim.pickup_centroid_latitude OR s.pickup_centroid_longitude<>trips_dim.pickup_centroid_longitude OR s.pickup_centroid_location<>trips_dim.pickup_centroid_location OR s.dropoff_centroid_latitude<>trips_dim.dropoff_centroid_latitude OR s.dropoff_centroid_longitude<>trips_dim.dropoff_centroid_longitude OR s.dropoff_centroid_location<>trips_dim.dropoff_centroid_location) 
            AND is_active = 1;
            """)

            print("Updating is_active and is_deleted for dim")
            cur.execute("""
            UPDATE trips_dim
            SET is_active = 0, is_deleted = 1, updated = now()
            WHERE NOT EXISTS (SELECT 1 FROM trips_stage WHERE trips_stage.trip_id = trips_dim.trip_id);
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
            cur.execute(""" INSERT INTO trips_final (created, updated, is_active, is_deleted,trip_id, trip_start_timestamp, trip_end_timestamp, trip_seconds,trip_miles, percent_time_chicago, percent_distance_chicago, pickup_census_tract, dropoff_census_tract,pickup_community_area, dropoff_community_area, fare, tip, additional_charges, trip_total,shared_trip_authorized, shared_trip_match, trips_pooled, pickup_centroid_latitude ,pickup_centroid_longitude, pickup_centroid_location, dropoff_centroid_latitude, dropoff_centroid_longitude,dropoff_centroid_location)
            SELECT now(), now(), 1, 0, s.trip_id, s.trip_start_timestamp, s.trip_end_timestamp, s.trip_seconds ,s.trip_miles, s.percent_time_chicago, s.percent_distance_chicago, s.pickup_census_tract, s.dropoff_census_tract,s.pickup_community_area, s.dropoff_community_area, s.fare, s.tip, s.additional_charges, s.trip_total,s.shared_trip_authorized, s.shared_trip_match, s.trips_pooled, s.pickup_centroid_latitude ,s.pickup_centroid_longitude, s.pickup_centroid_location, s.dropoff_centroid_latitude, s.dropoff_centroid_longitude,s.dropoff_centroid_location  
            FROM trips_stage s
            LEFT JOIN trips_final f 
            on s.trip_id = f.trip_id
            WHERE f.trip_id IS NULL;
            """)

            cur.execute(""" UPDATE trips_final 
            SET updated = now(), trip_start_timestamp=s.trip_start_timestamp, trip_end_timestamp=s.trip_end_timestamp, trip_seconds=s.trip_seconds ,trip_miles=s.trip_miles, percent_time_chicago=s.percent_time_chicago, percent_distance_chicago=s.percent_distance_chicago, pickup_census_tract=s.pickup_census_tract, dropoff_census_tract=s.dropoff_census_tract,pickup_community_area=s.pickup_community_area, dropoff_community_area=s.dropoff_community_area, fare=s.fare, tip=s.tip, additional_charges=s.additional_charges, trip_total=s.trip_total,shared_trip_authorized=s.shared_trip_authorized, shared_trip_match=s.shared_trip_match, trips_pooled=s.trips_pooled, pickup_centroid_latitude=s.pickup_centroid_latitude, pickup_centroid_longitude=s.pickup_centroid_longitude, pickup_centroid_location=s.pickup_centroid_location, dropoff_centroid_latitude=s.dropoff_centroid_latitude, dropoff_centroid_longitude=s.dropoff_centroid_longitude, dropoff_centroid_location=s.dropoff_centroid_location, is_active =1 
            FROM trips_stage s
            WHERE s.trip_id = trips_final.trip_id AND (s.trip_start_timestamp<>trips_final.trip_start_timestamp OR s.trip_end_timestamp<>trips_final.trip_end_timestamp OR s.trip_seconds<>trips_final.trip_seconds OR s.trip_miles<>trips_final.trip_miles OR s.percent_time_chicago<>trips_final.percent_time_chicago OR s.percent_distance_chicago<>trips_final.percent_distance_chicago OR s.pickup_census_tract<>trips_final.pickup_census_tract OR s.dropoff_census_tract<>trips_final.dropoff_census_tract OR s.pickup_community_area<>trips_final.pickup_community_area OR s.dropoff_community_area<>trips_final.dropoff_community_area OR s.fare<>trips_final.fare OR s.tip<>trips_final.tip OR s.additional_charges<>trips_final.additional_charges OR s.trip_total<>trips_final.trip_total OR s.shared_trip_authorized<>trips_final.shared_trip_authorized OR s.shared_trip_match<>trips_final.shared_trip_match OR s.trips_pooled<>trips_final.trips_pooled OR s.pickup_centroid_latitude<>trips_final.pickup_centroid_latitude OR s.pickup_centroid_longitude<>trips_final.pickup_centroid_longitude OR s.pickup_centroid_location<>trips_final.pickup_centroid_location OR s.dropoff_centroid_latitude<>trips_final.dropoff_centroid_latitude OR s.dropoff_centroid_longitude<>trips_final.dropoff_centroid_longitude OR s.dropoff_centroid_location<>trips_final.dropoff_centroid_location);
            """)

            cur.execute(""" UPDATE trips_final 
            SET is_active = 0, is_deleted = 1, updated = now()
            WHERE NOT EXISTS (SELECT 1 FROM trips_stage WHERE trips_final.trip_id = trips_stage.trip_id);
            """)
            conn.commit()
        except Exception as err:
            print('errrrrrrrrrrrrrrrrrrrrrr',err)
            traceback.print_exc()
            conn.rollback()
        finally:
            cur.close()
            conn.close()
if __name__ == '__main__':
    my_obj = Load_APIData('https://data.cityofchicago.org/resource/ukxa-jdd7.json?$Limit=5','postgresql','amandb', 'aman' , 12345,'localhost',5432)
    



# ?$where=%trip_start_timestamp%20between%20%272023-02-28T00:00:00%27%20and%20%272023-03-31T00:00:00%27

#  https://data.cityofchicago.org/Transportation/Transportation-Network-Providers-Trips-2023-/ukxa-jdd7
#################  https://data.cityofchicago.org/resource/ukxa-jdd7.json

# https://data.cityofchicago.org/Transportation/Transportation-Network-Provider-Trips-by-Month-202/krwv-nnih
################# https://data.cityofchicago.org/resource/n26f-ihde.json