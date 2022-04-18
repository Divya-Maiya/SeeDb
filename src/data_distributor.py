import configparser
import psycopg2
from psycopg2 import Error
import pandas as pd
import os
import numpy as np

def split_data():
    all_rows = pd.read_csv("../data/adult.data", sep=",")
    config = configparser.ConfigParser()
    config.read('../config/seedb_configs.ini')  
    splits = config['phased.execution.framework']['splits']
    print(all_rows.tail())
    df_split= np.array_split(all_rows,int(splits))
    for i in range(1,len(df_split)+1):
        df_split[i-1].to_csv("../data/test_split_{}.csv".format(i),encoding='utf-8', index=False)
    print("DONE")

def is_dir_empty(path):
    initial_count = 0
    if os.path.exists(path) and not os.path.isfile(path):
  
        if not os.listdir(path):
            return True
        else:
            for path2 in os.listdir(path):
                if os.path.isfile(os.path.join(path, path2)):
                    initial_count += 1
            return False if initial_count > 1 else True
    else:
        return True

def split_data_by_marital_status(cursor, connection):
    
    cursor.execute(open("../db_scripts/create_main_views.sql", "r").read())
    connection.commit()

    cursor.execute("select count(*) from married;")
    print("Married rows = " + str(cursor.fetchone()))
    
    cursor.execute("select count(*) from unmarried;")
    print("Unmarried rows = " + str(cursor.fetchone()))
    