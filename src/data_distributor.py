# Contributors: Chirag Uday Kamath (cukamath@umass.edu), Neha Prakash (nehaprakash@umass.edu)

import pandas as pd
import os
import numpy as np
import query_generator


# Routine to split the data for phased execution
# Contributor: Neha Prakash
def split_data(splits, file, dir, sep):
    all_rows = pd.read_csv("../data/" + dir + "/" + file, sep=sep)

    df_split = np.array_split(all_rows, splits)
    for i in range(1, len(df_split) + 1):
        df_split[i - 1].to_csv("../data/" + dir + "/test_split_{}.csv".format(i), encoding='utf-8', index=False,
                               sep=sep)


# Contributor: Neha Prakash
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


# Routine to generate splits
# Contributor: Chirag Uday Kamath
def generate_split_views(cursor, connection, splits, dir, sep, file):
    for i in range(1, splits + 1):
        if dir == 'census':
            query = query_generator.get_split_view_query(i)
        else:
            query = query_generator.get_split_view_dblp_query(i)
        cursor.execute(query)
        connection.commit()
        f = open('../data/{}/test_split_{}.csv'.format(dir, i), 'r', encoding='utf-8')
        cursor.copy_from(f, file + '{}'.format(i), sep=sep)
        connection.commit()
        f.close()
        # if dir == 'census':
        #     query = query_generator.get_married_umarried_view_generator_query(i)
        # else:
        #     query = query_generator.get_type0_type13_query(i)
        # cursor.execute(query)
        # connection.commit()


# Routine to split data by marital status - reference and target
# Currently unused since we implement query rewriting
# Contributor: Chirag Uday Kamath
def split_data_by_marital_status(cursor, connection):
    cursor.execute(open("../db_scripts/create_main_views_census.sql", "r").read())
    connection.commit()

    cursor.execute("select count(*) from married;")
    print("Married rows = " + str(cursor.fetchone()))

    cursor.execute("select count(*) from unmarried;")
    print("Unmarried rows = " + str(cursor.fetchone()))
