import configparser
import sys
import math
import db_connector
import db_disconnector
import data_distributor
import query_utils
import query_generator
import distance_utils
import visualize
import os

config = configparser.ConfigParser()
config.read('../config/seedb_configs.ini')
path = config['local.paths']['basepath']
sys.path.insert(0, path + '/connectors')
sys.path.insert(1, path + '/src')
splits = int(config['phased.execution.framework']['splits'])

# Census Dataset
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


def main(measure):
    try:
        cursor, connection = db_connector.setup_connection('seedb_database_census')
        queries = query_utils.generate_aggregate_queries(dim_attr, measure_attr, agg_functions, "Census")
        print("Total aggregate views: {}".format(len(queries)))

        aggregate_views = query_utils.generate_aggregate_views(dim_attr, measure_attr, agg_functions)

        if data_distributor.is_dir_empty("../data/census"):
            data_distributor.split_data(splits, "adult.data", "census", ',')

        data_distributor.generate_split_views(cursor, connection, splits, 'census', ',', 'split_view')

        # Phased Execution
        bounds = {}
        for phase in range(splits):

            current_phase = phase + 1
            dist_views = {}
            for a in aggregate_views:

                # Sharing based optimization
                query_params = ""
                for m in aggregate_views[a]:
                    for f in aggregate_views[a][m]:
                        query_params += "{}({}) as {}${}, ".format(f, m, f, m)

                query_params = query_params[:-2]

                # Extension - Query rewriting. Use a single query to fetch data from target and reference dataset
                query = query_generator.get_target_reference_merged_query_census(a, query_params, phase)

                cursor.execute(query)
                rows = cursor.fetchall()

                # Find the distance based on the utility measure - KL Divergence, Earth Mover's distance etc.
                dists = distance_utils.find_distance(rows, [desc[0] for desc in cursor.description], measure)

                for agg_key, dist in dists.items():
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

                # Pruning based optimization
                for m in aggregate_views[a]:
                    for f in aggregate_views[a][m]:
                        if phase == 0:
                            continue

                        # Hoeffding-Serfling inequality: m = current_phase, N = splits, delta = 1e-5

                        eps_m = math.sqrt((1 - (current_phase - 1) / splits) * (
                                2 * math.log(math.log(current_phase)) + math.log(math.pow(math.pi, 2) / (3 * delta)))
                                          / (2 * current_phase))

                        # Get upper and lower bounds
                        lower_bound = dist_views[a][m][f] / current_phase - eps_m
                        upper_bound = dist_views[a][m][f] / current_phase + eps_m

                        bounds[a, m, f] = (lower_bound, upper_bound)

            # Sort the bounds
            sorted_conf_intervals = {k: v for k, v in sorted(bounds.items(), key=lambda item: -1 * item[1][1])}

            if len(sorted_conf_intervals) < k:
                continue

            # Find lowest lower bound
            lowestLowerBound = 0.0
            for i in list(sorted_conf_intervals.items())[:k]:
                lowestLowerBound = min(lowestLowerBound, i[1][0])

            # Iterate and prune out
            for s in list(sorted_conf_intervals.items())[k:]:
                if s[1][1] < lowestLowerBound or current_phase == splits:
                    del bounds[s[0]]

                    func = aggregate_views[s[0][0]][s[0][1]]
                    func.remove(s[0][2])
                    aggregate_views[s[0][0]][s[0][1]] = func

                    if len(aggregate_views[s[0][0]][s[0][1]]) == 0:
                        del aggregate_views[s[0][0]][s[0][1]]

                    if len(aggregate_views[s[0][0]]) == 0:
                        del aggregate_views[s[0][0]]

            # Get count of current views in consideration
            count = 0
            for a in aggregate_views:
                for m in aggregate_views[a]:
                    count += len(aggregate_views[a][m])

        print(aggregate_views)

        cursor.execute(open("../db_scripts/prepare_census_data.sql", "r").read())
        f = open('../data/census/adult.data', 'r')
        cursor.copy_from(f, 'census', sep=',')
        f.close()

        # Visualize the top k views left after all phases
        for a in aggregate_views:
            for m in aggregate_views[a]:
                for f in aggregate_views[a][m]:
                    visualize.visualize_census_data(cursor, a, f, m)

    finally:
        db_disconnector.teardown_connection(cursor, connection)


measure = 'kl_divergence'
if len(sys.argv) == 2 and sys.argv[1] in ['kl_divergence', 'emd_distance', 'js_divergence_distance',
                                          'euclidean_distance']:
    measure = sys.argv[1]
else:
    print("No Distance given or incorrect distance used. Defaulting to KL Divergence")
main(measure)
