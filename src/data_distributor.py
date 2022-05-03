import pandas as pd
import os
import numpy as np
import query_generator


# Routine to split the data for phased execution
def split_data(splits):
    all_rows = pd.read_csv("../data/census/adult.data", sep=",")

    df_split = np.array_split(all_rows, splits)
    for i in range(1, len(df_split) + 1):
        df_split[i - 1].to_csv("../data/census/test_split_{}.csv".format(i), encoding='utf-8', index=False)


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
def generate_split_views(cursor, connection, splits):
    for i in range(1, splits + 1):
        query = query_generator.get_split_view_query(i)
        cursor.execute(query)
        connection.commit()
        f = open('../data/census/test_split_{}.csv'.format(i), 'r')
        cursor.copy_from(f, 'split_view{}'.format(i), sep=',')
        connection.commit()
        f.close()
        query = query_generator.get_married_umarried_view_generator_query(i)
        cursor.execute(query)
        connection.commit()


# Routine to split data by marital status - reference and target
# Currently unused since we implement query rewriting
def split_data_by_marital_status(cursor, connection):
    cursor.execute(open("../db_scripts/create_main_views_census.sql", "r").read())
    connection.commit()

    cursor.execute("select count(*) from married;")
    print("Married rows = " + str(cursor.fetchone()))

    cursor.execute("select count(*) from unmarried;")
    print("Unmarried rows = " + str(cursor.fetchone()))
