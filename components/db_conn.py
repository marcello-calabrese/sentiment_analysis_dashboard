import psycopg2

CONN = psycopg2.connect(
    host = 'localhost',
    dbname = 'Cars_Review',
    user = 'postgres',
    password = 'Lorenzo_22',
    port = '5432')