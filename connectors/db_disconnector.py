# Routine to close DB connection
def teardown_connection(cursor, connection):
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")
