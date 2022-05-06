# Contributor: Neha Prakash (nehaprakash@umass.edu), Chirag Uday Kamath (cukamath@umass.edu)
import configparser
import psycopg2

config = configparser.ConfigParser()
config.read('../config/seedb_configs.ini')
config.sections()


# Routine to connect to the Db
def setup_connection(db_name):
    # Connect to an existing database
    connection = psycopg2.connect(user=config['seedb.postgresql']['seedb_user'],
                                  password=config['seedb.postgresql']['seedb_password'],
                                  host=config['seedb.postgresql']['seedb_host'],
                                  database=config['seedb.postgresql'][db_name])

    # Create a cursor to perform database operations
    cursor = connection.cursor()

    # # Print PostgreSQL details
    # print("PostgreSQL server information")
    # print(connection.get_dsn_parameters(), "\n")

    # Executing a SQL query
    cursor.execute("SELECT version();")

    # Fetch result
    record = cursor.fetchone()
    print("You are connected to - ", record, "\n")

    return cursor, connection
