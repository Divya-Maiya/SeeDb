import configparser
import psycopg2
from psycopg2 import Error
import sys

config = configparser.ConfigParser()
config.read('../config/seedb_configs.ini')  
path = config['local.paths']['basepath']
sys.path.insert(0, path+'/connectors')
sys.path.insert(1, path+'/src')
splits = config['phased.execution.framework']['splits']
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
import data_distributor
import query_utils
try:
    cursor, connection = db_connector.setup_connection()
    queries = query_utils.generate_aggregate_queries(dim_attr, measure_attr, agg_functions, "Census")
    for q in queries:
        print(q)

    cursor.execute("select count(*) from census")
    rows = cursor.fetchone()
    print(rows)
    data_distributor.split_data_by_marital_status(cursor, connection)
    if(data_distributor.is_dir_empty("../data")):
        data_distributor.split_data(splits)
    
    data_distributor.generate_split_views(cursor, connection, splits)
    cursor.execute("select count(*) from split_view1")
    rows = cursor.fetchone()
    print(rows)   
    cursor.execute("select count(*) from split_view2")
    rows2 = cursor.fetchone()
    print(rows2) 
    cursor.execute("select count(*) from split_view3")
    rows3 = cursor.fetchone()
    print(rows3) 
except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL", error)
finally:
    db_disconnector.teardown_connection(cursor, connection)