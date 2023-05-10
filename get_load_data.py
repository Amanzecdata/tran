import requests
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import json
import traceback

class Load_APIData:
    def __init__(self, url, dbprovider, dbname, user, password, host='localhost', port=5432):
        self.url = url
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.dbprovider = dbprovider
        self.data = self.get_data()
        self.insert_data(self.data)

    def get_data(self):
        response = requests.get(self.url)
        print("Data get successfully")
        return response.json()

    def insert_data(self, data):
        try:
            print("Establishing Connection")
            conn = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            cur = conn.cursor()
            print("Connection established\ninserting data into trips")

            # Insert data into table
            db_string = f"{self.dbprovider}://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"
            engine = create_engine(db_string)
            response = requests.get(self.url).json()
            df = pd.DataFrame(response)

            # Convert the pickup_centroid_location and dropoff_centroid_location columns to JSON strings
            df['pickup_centroid_location'] = df['pickup_centroid_location'].apply(json.dumps)
            df['dropoff_centroid_location'] = df['dropoff_centroid_location'].apply(json.dumps)
            df.to_sql(name='trips', con=engine, if_exists='append', index=False)
            print("---- DONE ----")
        except Exception as err:
            print("errrrrrrrrrrrrrrrrrrrr",err)
            traceback.print_exc()
            return False
        finally:
            conn.commit()
            cur.close()
            conn.close()

if __name__ == '__main__':
    my_obj = Load_APIData('https://data.cityofchicago.org/resource/ukxa-jdd7.json?$Limit=20','postgresql','amandb', 'aman' , 12345)
    



# ?$where=%trip_start_timestamp%20between%20%272023-02-28T00:00:00%27%20and%20%272023-03-31T00:00:00%27

#  https://data.cityofchicago.org/Transportation/Transportation-Network-Providers-Trips-2023-/ukxa-jdd7

#################  https://data.cityofchicago.org/resource/ukxa-jdd7.json

# https://data.cityofchicago.org/Transportation/Transportation-Network-Provider-Trips-by-Month-202/krwv-nnih

################# https://data.cityofchicago.org/resource/n26f-ihde.json


