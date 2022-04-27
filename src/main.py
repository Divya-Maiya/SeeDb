import configparser
import psycopg2
from psycopg2 import Error
import sys
import math

config = configparser.ConfigParser()
config.read('../config/seedb_configs.ini')
path = config['local.paths']['basepath']
sys.path.insert(0, path + '/connectors')
sys.path.insert(1, path + '/src')
splits = int(config['phased.execution.framework']['splits'])
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

agg_functions = ["sum", "min", "max", "avg", "count"]
dim_attr = ["workclass", "education", "occupation", "relationship", "race", "sex", "native_country", "salary"]
measure_attr = ["age", "fnlwgt", "capital_gain", "capital_loss", "hours_per_week"]
delta = 1e-5
k = 5

import db_connector
import db_disconnector
import data_distributor
import query_utils
import query_generator

try:
    cursor, connection = db_connector.setup_connection()
    queries = query_utils.generate_aggregate_queries(dim_attr, measure_attr, agg_functions, "Census")
    print("Total aggregate views: {}".format(len(queries)))

    aggregate_views = query_utils.generate_aggregate_views(dim_attr, measure_attr, agg_functions)
    # print(aggregate_views)
    # cursor.execute("select count(*) from census")
    # rows = cursor.fetchone()
    # print(rows)
    # data_distributor.split_data_by_marital_status(cursor, connection)

    if data_distributor.is_dir_empty("../data"):
        data_distributor.split_data(splits)

    data_distributor.generate_split_views(cursor, connection, splits)

    # Phased Execution
    # phase_wise_dist = []
    bounds = {}
    for phase in range(splits):

        dist_views = {}
        for a in aggregate_views:

            # Sharing based optimization
            query_params = ""
            for m in aggregate_views[a]:
                for f in aggregate_views[a][m]:
                    query_params += "{}({}) as {}${}, ".format(f, m, f, m)

            query_params = query_params[:-2]

            query = query_generator.get_target_reference_merged_query(a, query_params, phase)

            cursor.execute(query)
            rows = cursor.fetchall()

            dists = query_utils.transform_data(rows, [desc[0] for desc in cursor.description])

            for agg_key, dist in dists.items():
                # print(type(agg_key))
                parts = agg_key.split("$")
                f = parts[0]
                m = parts[1]

                if a not in dist_views:
                    dist_views[a] = {}

                if m not in dist_views[a]:
                    dist_views[a][m] = {}

                if f not in dist_views[a][m]:
                    dist_views[a][m][f] = 0

                dist_views[a][m][f] += dist

            # phase_wise_dist.append(dist_views)

            # Pruning based optimization
            for m in aggregate_views[a]:
                for f in aggregate_views[a][m]:
                    if phase == 0:
                        continue
                    # TODO change
                    em = math.sqrt((1 - (phase + 1 - 1) / splits) * (
                            2 * math.log(math.log(phase + 1)) + math.log(math.pi ** 2 / (3 * delta))) / (
                                           2 * (phase + 1)))
                    lower_bound = dist_views[a][m][f] / (phase + 1) - em
                    upper_bound = dist_views[a][m][f] / (phase + 1) + em

                    # if a not in bounds:
                    #     bounds[a] = {}
                    #
                    # if m not in bounds[a]:
                    #     bounds[a][m] = {}
                    #
                    # if f not in bounds[a][m]:
                    #     bounds[a][m][f] = ()

                    bounds[a, m, f] = (lower_bound, upper_bound)

        # print(bounds)
        # print()
        # Sort
        sorted_bounds = {k: v for k, v in sorted(bounds.items(), key=lambda item: -1 * item[1][1])}
        # sorted_bounds.sort(key=lambda tup: -1 * tup[1][1])
        # print(sorted_bounds)

        # TODO Ensure k views
        if len(sorted_bounds) < k:
            continue

        lowestLowerBound = 0.0
        for i in list(sorted_bounds.items())[:k]:
            lowestLowerBound = min(lowestLowerBound, i[1][0])

        for s in list(sorted_bounds.items())[k:]:
            if s[1][1] < lowestLowerBound or phase == splits - 1:
                del bounds[s[0]]
                func = aggregate_views[s[0][0]][s[0][1]]
                func.remove(s[0][2])
                aggregate_views[s[0][0]][s[0][1]] = func
                if len(aggregate_views[s[0][0]][s[0][1]]) == 0:
                    del aggregate_views[s[0][0]][s[0][1]]
                if len(aggregate_views[s[0][0]]) == 0:
                    del aggregate_views[s[0][0]]
        count  = 0
        for a in aggregate_views:
            for m in aggregate_views[a]:
                count += len(aggregate_views[a][m])
        # print(count)

    print(aggregate_views)
    final_views = []
    c = 0
    for a in aggregate_views:
            for m in aggregate_views[a]:
                for f in aggregate_views[a][m]:
                    if c==0:
                        query_utils.visualize_data(connection, cursor, a,f,m)
                    c+=1
    


# except (Exception, Error) as error:
#     print("Error while connecting to PostgreSQL", error)
finally:
    db_disconnector.teardown_connection(cursor, connection)
