import wget
import os 
from pathlib import Path
import psycopg2
import traceback

class Load_CSVData:
    global conn,cur
    print("Establishing Connection")
    conn = psycopg2.connect(dbname='amandb', user='aman', password=12345, host='localhost', port=5432)
    cur = conn.cursor()
    print("Connection established")

    def __init__(self,remote_url,file_name,file_ext,full_path,table_name,row_count,partition_num,file_part_name):
        self.remote_url= remote_url
        self.file_name = file_name
        self.file_ext = file_ext
        self.table_name = table_name
        self.row_count = row_count
        self.partition_num = partition_num
        self.file_part_name = file_part_name

    def set_data(self):
        command = f'''head -n 1 {full_path} > header.csv && tail -n +2 {full_path} | split -l {partition_num} -d --filter='cat header.csv - > $FILE.csv' - {file_part_name}''' 
        os.system(command)
        os.system(f'rm {full_path}')
        for i in range(self.row_count//(self.partition_num+1) if self.row_count%(self.partition_num+1) == 0 else self.row_count//(self.partition_num+1)+1):
            self.full_path =  f'{only_file_path}/{file_part_name}0{i}.{file_ext}'
            print(i,self.full_path)
            my_obj.get_load_data_csv(self.full_path)
            try:
                os.system(f'rm {self.full_path}')
            except Exception:
                print("Exxxxxxxxxxxx",Exception)

    def get_load_data_csv(self,full_path):                                     
        try:
            cur.execute(f"""COPY {self.table_name} FROM '{full_path}' DELIMITER ',' CSV HEADER;""")
            conn.commit()
            print("_________DONE___________")
        except Exception as err:
            print(err)
            traceback.print_exc()
            conn.rollback()

remote_url = 'https://data.cityofchicago.org/resource/n26f-ihde.csv'
base_url = 'https://data.cityofchicago.org/resource/n26f-ihde.csv'

file_full_name = wget.download(remote_url, bar=None)
file_name = Path(file_full_name)
full_path = file_name.resolve()
only_file_path = os.path.dirname(full_path)
file_name = file_full_name.split(".")[0]
file_ext = file_full_name.split(".")[1]
table_name = 'trips_data'
file_part_name = 'aman_'
partition_num = int(input("enter number of records for file partition"))

with open(file_full_name, 'r') as file:
    row_count = len(file.readlines())
file.close()
print(row_count)

my_obj = Load_CSVData(remote_url,file_name,file_ext,full_path,table_name,row_count,partition_num,file_part_name)
my_obj.set_data()
cur.close()
conn.close()

# offset = 100
# limit = 50 
# url = f'{base_url}?$offset={offset}&$limit={limit}'

