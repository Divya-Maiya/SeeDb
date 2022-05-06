# Routine to close DB connection
# Contributor: Chirag Uday Kamath (cukamath@umass.edu), Neha Prakash (nehaprakash@umass.edu)
def teardown_connection(cursor, connection):
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")
