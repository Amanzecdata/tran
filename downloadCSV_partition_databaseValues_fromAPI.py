import wget
import os 
from pathlib import Path
import psycopg2
import traceback
import csv

class Load_CSVData:
    global conn,cur
    print("Establishing Connection")
    conn = psycopg2.connect(dbname='amandb', user='aman', password=12345, host='localhost', port=5432)
    cur = conn.cursor()
    print("Connection established")

    def __init__(self,remote_url,file_part_name,tableName,file_extension,file_name,splitted_csv_path,only_file_path,full_path_with_fileName,partition_num):
        self.remote_url= remote_url
        self.file_name = file_name
        self.file_ext = file_extension
        self.table_name = tableName
        self.partition_num = partition_num
        self.file_part_name = file_part_name
        self.full_path_with_fileName = full_path_with_fileName
        self.splitted_csv_path = splitted_csv_path
        self.only_file_path = only_file_path
        print(file_name,file_extension,tableName,partition_num,file_part_name,full_path_with_fileName,splitted_csv_path,only_file_path)

    def set_data(self):
        file_full_name = wget.download(self.remote_url, bar=None)
        command = f'''head -n 1 {self.full_path_with_fileName} > header.csv && tail -n +2 {self.full_path_with_fileName} | split -l {partition_num} -d --filter='cat header.csv - > {self.splitted_csv_path}$FILE.csv' - {self.file_part_name}''' 
        os.system(command)
        with open(self.full_path_with_fileName, 'r') as file:
            reader = csv.reader(file)
            row_count = len(list(reader))
            print(row_count)
        os.system(f'rm {self.file_name}')
        for i in range(row_count//(self.partition_num+1) if row_count%(self.partition_num+1) == 0 else row_count//(self.partition_num+1)+1):
            self.full_path =  f'{self.only_file_path}/{file_part_name}0{i}.{self.file_ext}'
            print(i,self.full_path)
            my_obj.get_load_data_csv(self.full_path)
            try:
                os.system(f'rm {self.full_path}')
            except Exception:
                print("Exxxxxxxxxxxx",Exception)

    def get_load_data_csv(self,full_path):                                     
        try:
            cur.execute(f"""COPY {self.table_name} FROM '{self.full_path}' DELIMITER ',' CSV HEADER;""")
            conn.commit()
            print("_________DONE___________")
        except Exception as err:
            print(err)
            traceback.print_exc()
            conn.rollback()

remote_url = 'https://data.cityofchicago.org/resource/n26f-ihde.csv'

cur.execute("""SELECT vals from trips_param 
               where keys='file_part_name';""")
file_part_name = cur.fetchone()
file_part_name = file_part_name[0].strip('(),')

cur.execute("""SELECT vals from trips_param 
               where keys='partition_num';""")
partition_num = cur.fetchone()
partition_num = int(partition_num[0].strip('(),'))

cur.execute("""SELECT vals from trips_param 
               where keys='full_path_with_fileName';""")
full_path_with_fileName = cur.fetchone()
full_path_with_fileName = full_path_with_fileName[0].strip('(),')

cur.execute("""SELECT vals from trips_param 
               where keys='only_file_path';""")
only_file_path = cur.fetchone()
only_file_path = only_file_path[0].strip('(),')

cur.execute("""SELECT vals from trips_param 
               where keys='splitted_csv_path';""")
splitted_csv_path = cur.fetchone()
splitted_csv_path = splitted_csv_path[0].strip('(),')

cur.execute("""SELECT vals from trips_param 
               where keys='file_name';""")
file_name = cur.fetchone()
file_name = file_name[0].strip('(),')

cur.execute("""SELECT vals from trips_param 
               where keys='file_extension';""")
file_extension = cur.fetchone()
file_extension = file_extension[0].strip('(),')

cur.execute("""SELECT vals from trips_param 
               where keys='tableName';""")
tableName = cur.fetchone()
tableName = tableName[0].strip('(),')

    
my_obj = Load_CSVData(remote_url,file_part_name,tableName,file_extension,file_name,splitted_csv_path,only_file_path,full_path_with_fileName,partition_num)
my_obj.set_data()
cur.close()
conn.close()

# print(tableName,file_extension,file_name,splitted_csv_path,only_file_path,full_path_with_fileName,partition_num,file_part_name)
