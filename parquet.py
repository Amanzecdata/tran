import pyarrow.parquet as pq


# Specify the path to your "cities.parquet" file
file_path = '/home/hp/Downloads/cities.parquet'

# Read the Parquet file
table = pq.read_table(file_path)

# Convert the table to a Pandas DataFrame
df = table.to_pandas()

# Print the first few rows of the DataFrame
print(df.head())
