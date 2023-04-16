# Ingest the data into the postgres database

# Import the necessary libraries

import pandas as pd
import psycopg2
import os
from path import PATH
from db_conn import CONN


# read the data from the excel file

path = PATH

df = pd.read_csv(path, encoding='utf-8')

# create a function to connect to the database, create a table and insert the data, if the table already exists, it will be dropped and created again

# connect to the database

conn = CONN

# save the dataframe to the database

# Create a cursor object
cur = conn.cursor()

# Generate the SQL statement to insert the data

table_name = 'cars_raw'
cols = []
for col, dtype in zip(df.columns, df.dtypes):
    if dtype == 'int64':
        cols.append(f'{col} INTEGER')
    elif dtype == 'float64':
        cols.append(f'{col} FLOAT')
    else:
        cols.append(f'{col} VARCHAR(10000)')
cols = ', '.join(cols)

placeholders = ', '.join(['%s'] * len(df.columns))

create_table = f"CREATE TABLE IF NOT EXISTS {table_name} ({cols})"
insert_data = f"INSERT INTO {table_name} VALUES ({placeholders})"

# Execute the SQL statements
cur.execute(create_table)
for i, row in df.iterrows():
    cur.execute(insert_data, tuple(row))
    
# Commit the changes and close the cursor
conn.commit()
cur.close()

# Close the connection
conn.close()