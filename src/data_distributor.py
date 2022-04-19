import configparser
import psycopg2
from psycopg2 import Error
import pandas as pd
import os
import numpy as np

def split_data(splits):
    all_rows = pd.read_csv("../data/adult.data", sep=",")
    
    df_split= np.array_split(all_rows,int(splits))
    for i in range(1,len(df_split)+1):
        df_split[i-1].to_csv("../data/test_split_{}.csv".format(i),encoding='utf-8', index=False)

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

def generate_split_views(cursor, connection, splits):
    for i in range(1, int(splits)+1):
        cursor.execute("""
                       create temp table split_view{} (age real, workclass text, fnlwgt real, education text, education_num real, marital_status text, occupation text, relationship text, race text, sex text, capital_gain real, capital_loss real, hours_per_week real, native_country text, economic_indicator text);
                       """.format(i, i, i))
        connection.commit()
        f = open('../data/test_split_{}.csv'.format(i), 'r')
        cursor.copy_from(f, 'split_view{}'.format(i), sep=',')
        connection.commit()
        f.close()
        cursor.execute("""
                       drop table if exists split_married_{};
                       drop table if exists split_unmarried_{};
                       create table split_married_{} as (select * from split_view{} where marital_status in (' Married-AF-spouse', ' Married-civ-spouse', ' Married-spouse-absent',' Separated'));
                       create table split_unmarried_{} as (select * from split_view{} where marital_status in (' Never-married', ' Widowed',' Divorced'));
                       """.format(i,i,i,i,i,i))
        connection.commit()
        
    

def split_data_by_marital_status(cursor, connection):
    
    cursor.execute(open("../db_scripts/create_main_views.sql", "r").read())
    connection.commit()

    cursor.execute("select count(*) from married;")
    print("Married rows = " + str(cursor.fetchone()))
    
    cursor.execute("select count(*) from unmarried;")
    print("Unmarried rows = " + str(cursor.fetchone()))
    