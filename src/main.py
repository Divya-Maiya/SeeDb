import configparser
import psycopg2
from psycopg2 import Error
import sys

config = configparser.ConfigParser()
config.read('../config/seedb_configs.ini')  
path = config['local.paths']['basepath']
sys.path.insert(0, path+'/connectors')

import db_connector
import db_disconnector
try:
    cursor, connection = db_connector.setupConnection()
    cursor.execute("select * from test")
    rows = cursor.fetchall()
    for r in rows:
        print(r)
except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL", error)
finally:
    db_disconnector.teardownConnection(cursor, connection)