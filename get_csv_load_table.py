import requests
import csv
import psycopg2
import traceback

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
            cur.execute(f"""COPY trips_stage1 FROM {filepath} DELIMITER ',' CSV HEADER;""")
            conn.commit()
        except Exception as err:
            print(err)
            traceback.print_exc()
            conn.rollback()
        finally:
            cur.close()
            conn.close()
my_obj = Load_CSVData('amandb', 'aman', 12345, 'localhost', 5432)
for i in range(15):
    filepath = '/home/hp/Desktop/chintu/tran/trips_{:02d}.csv'.format(i)
    my_obj.get_load_data_csv(filepath) 












# filepath='/home/hp/Desktop/chintu/tran/top_100.csv'
# my_obj.get_load_data_csv(filepath)  
# my_obj.insert_trip_dim()

# split -l 50000 -d --additional-suffix=.csv Transportation_Network_Providers_Trips_2023.csv trips_

# split -l 50000 -d --filter='tail -n +2 > $FILE.csv' Transportation_Network_Providers_Trips_2023.csv trips_

# CORRECT_COMMAND : split -l 50001 -d --filter='cat > $FILE.csv' Transportation_Network_Providers_Trips_2023.csv trips_

# 747721    -----------1f09b3ee2a57e7ede5e33f099cbe34dbe9926490	01/29/2023 12:45:00 PM	01/29/2023 12:45:00 PM	177	1	1	1			41	41	12.5	0	1.26	13.76	false	false	1	41.79{


# head -n 100 Transportation_Network_Providers_Trips_2023.csv > top_100.csv


#####################################################################################


    # def get_data_csv(self):
    #     url = "https://data.cityofchicago.org/api/views/n26f-ihde/rows.csv?$Limit=20"
    #     response = requests.get(url)
    #     lines = response.text.splitlines()
    #     csv_reader = csv.reader(lines)
    #     header = next(csv_reader) # Assuming the first line is the header
    #     data = []
    #     for i, row in enumerate(csv_reader):
    #         data.append(row)
    #     print(data)



    # for i, row in enumerate(csv_reader):
    #     if i == 0: # Skipping header row
    #         continue
    #     cur.execute(
    #         "INSERT INTO trips_stage1 (trip_id, trip_start_timestamp, trip_end_timestamp, trip_seconds,trip_miles, percent_time_chicago, percent_distance_chicago, pickup_census_tract, dropoff_census_tract,pickup_community_area, dropoff_community_area, fare, tip, additional_charges, trip_total,shared_trip_authorized, shared_trip_match, trips_pooled, pickup_centroid_latitude ,pickup_centroid_longitude, pickup_centroid_location, dropoff_centroid_latitude, dropoff_centroid_longitude,dropoff_centroid_location) "
    #         "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", row
    #     )



######################################################################################


# ?$where=%trip_start_timestamp%20between%20%272023-02-28T00:00:00%27%20and%20%272023-03-31T00:00:00%27

#  https://data.cityofchicago.org/Transportation/Transportation-Network-Providers-Trips-2023-/ukxa-jdd7
#################  https://data.cityofchicago.org/resource/ukxa-jdd7.json

# https://data.cityofchicago.org/Transportation/Transportation-Network-Provider-Trips-by-Month-202/krwv-nnih
################# https://data.cityofchicago.org/resource/n26f-ihde.json