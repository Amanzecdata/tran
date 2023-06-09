import fastavro
import pandas as pd

avro_file = "/home/hp/Downloads/100.variants.avro"

import fastavro

# Specify the path to the Avro file
# avro_file = "/path/to/file.avro"

# Open the Avro file in read mode
with open(avro_file, "rb") as file:
    # Read the Avro records from the file
    avro_reader = fastavro.reader(file)
    for record in avro_reader:
        # Process each record as needed
        print(record)


# with open(avro_file, "rb") as file:
#     # Read the Avro records from the file
#     avro_reader = fastavro.reader(file)
   
#     # Convert Avro records to a list of dictionaries
#     records = [record for record in avro_reader]
    
# # Create a pandas DataFrame from the Avro records
# df = pd.DataFrame.from_records(records)

# print(df)
