# Routine to close DB connection
# Contributor: Chirag Uday Kamath, Neha Prakash
def teardown_connection(cursor, connection):
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")
