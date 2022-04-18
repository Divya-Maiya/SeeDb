import configparser
import psycopg2
from psycopg2 import Error

def teardown_connection(cursor, connection):
    if (connection):
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")