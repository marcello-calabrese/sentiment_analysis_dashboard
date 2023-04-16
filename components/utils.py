# Main functions used in the project



# create a function to clean the data
def clean_data(df):
    # replace Rating with the median of the Rating column
    df['rating'].fillna(df['rating'].median(), inplace=True)
    # drop the nan values
    df = df.dropna()
    # drop the duplicates
    df = df.drop_duplicates()
    # drop the rows with value 'NaN' in the column 'author_name'
    df = df[df['author_name'] != 'NaN']
    # reset the index
    df = df.reset_index(drop=True)    
    # return the cleaned dataframe
    return df

####################################################################################################################

# function to create a sample of 2000 rows

def create_sample(df, n):
    '''
    This function creates a sample of n rows from the dataframe
    '''
    # create a sample of 2000 rows with random state 42
    df_sample = df.sample(n, random_state=42)
    
    # reset the index
    df_sample = df_sample.reset_index(drop=True)
    # return the sample
    return df_sample

####################################################################################################################


# function to save the sample in the postgresql database as a table

def insert_data_to_pgdb(conn, df_sample, table_name='cars_cleaned_sample'):
    # Create a cursor object
    cur = conn.cursor()
    
    # Generate the SQL statement to insert the data
    cols = []
    for col, dtype in zip(df_sample.columns, df_sample.dtypes):
        if dtype == 'int64':
            cols.append(f'{col} INTEGER')
        elif dtype == 'float64':
            cols.append(f'{col} FLOAT')
        else:
            cols.append(f'{col} VARCHAR(10000)')
    cols = ', '.join(cols)

    placeholders = ', '.join(['%s'] * len(df_sample.columns))

    create_table = f"CREATE TABLE IF NOT EXISTS {table_name} ({cols})"
    insert_data = f"INSERT INTO {table_name} VALUES ({placeholders})"
    
    # Execute the SQL statements
    cur.execute(create_table)
    for i, row in df_sample.iterrows():
        cur.execute(insert_data, tuple(row))
        
    

    # Commit the changes and close the cursor
    conn.commit()
    cur.close()


###############################################################################################

# function to save the sentiment dataframe in the postgresql database as a table

def insert_sentiment_data_to_pgdb(conn, df, table_name='cars_cleaned_sentiment'):
    # Create a cursor object
    cur = conn.cursor()
    
    # Generate the SQL statement to insert the data
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


