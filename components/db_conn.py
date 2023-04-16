import psycopg2

CONN = psycopg2.connect(
    host = 'localhost',
    dbname = 'Cars_Review',
    user = 'postgres',
    password = 'XXXXX',
    port = '5432')
