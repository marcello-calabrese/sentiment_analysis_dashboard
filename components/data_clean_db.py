# in this file, we clean the data, create a sample of 2000 rows and store it in a database as a table called cars_sample

# import the necessary libraries
import pandas as pd
import numpy as np 
import psycopg2
from utils import clean_data, create_sample, insert_data_to_pgdb
import os 
from db_conn import CONN

# import library to avoid warnigns
import warnings

# ignore warnings
warnings.filterwarnings('ignore')

# Set up a connection to the postgresql database
conn = CONN

# Execute a SQL query to retrieve the data you need
sql_query = "SELECT * FROM cars_raw"
df = pd.read_sql_query(sql_query, conn)

# execute the cleaning function
df = clean_data(df)


# execute the function to create a sample of 2000 rows

df_sample = create_sample(df, 2000)

# save the sample to the table cars_cleaned_sample using the function 

insert_data_to_pgdb(conn, df_sample, table_name='cars_cleaned_sample')





