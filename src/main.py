import configparser
import psycopg2
from psycopg2 import Error
import sys

config = configparser.ConfigParser()
config.read('../config/seedb_configs.ini')  
path = config['local.paths']['basepath']
sys.path.insert(0, path+'/connectors')

# Dataset
#   age INTEGER,
#   workclass CHAR(50),
# 	fnlwgt INTEGER,
# 	education CHAR(50),
# 	education_num INTEGER,
# 	marital_status CHAR(50),
# 	occupation CHAR(50),
# 	relationship CHAR(50),
# 	race CHAR(50),
# 	sex CHAR(20),
# 	capital_gain INTEGER,
# 	capital_loss INTEGER,
# 	hours_per_week INTEGER,
# 	native_country CHAR(50),
# 	salary CHAR(50)

agg_functions = ["SUM", "MIN", "MAX", "AVG", "COUNT"]
dim_attr = ["workclass", "education", "marital_status", "occupation", "relationship", "race", "sex", "native_country", "salary"]
measure_attr = ["age", "fnlwgt", "capital_gain", "capital_loss", "hours_per_week"]

import db_connector
import db_disconnector
import query_utils
try:
    cursor, connection = db_connector.setupConnection()
    # cursor.execute("select * from Census limit 10")
    # rows = cursor.fetchall()
    # for r in rows:
    #     print(r)

    queries = query_utils.generate_aggregate_queries(dim_attr, measure_attr, agg_functions, "Census")
    for q in queries:
        print(q)

except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL", error)
finally:
    db_disconnector.teardownConnection(cursor, connection)