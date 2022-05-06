# Contributors: Neha Prakash(nehaprakash@umass.edu), Divya Maiya (dmaiya@umass.edu)

import pandas as pd


# Contributor: Divya Maiya
# Routine to generate all aggregate queries
def generate_aggregate_queries(A, M, F, table):
    # A - Dimension attributes (group by), M - Measure attribute (aggregate), F - Aggregate functions
    print("Generating aggregate queries")

    queries = []
    for a in A:
        for m in M:
            for f in F:
                queries.append("SELECT {}, {}({}) FROM {} GROUP BY {}".format(a, f, m, table, a))

    return queries


# Contributor: Divya Maiya
# Routine to generate data structure to store all aggregate views
def generate_aggregate_views(A, M, F):
    # A - Dimension attributes (group by), M - Measure attribute (aggregate), F - Aggregate functions

    views = {}
    for a in A:
        for m in M:
            for f in F:
                if a not in views:
                    views[a] = {}
                if m not in views[a]:
                    views[a][m] = set()
                views[a][m].add(f)

    return views


# Contributor: Neha Prakash
# Routine to execute sql queries
def execute_queries(cursor, queries):
    data = []
    for query in queries:
        cursor.execute(query)
        data.append(cursor.fetchall())

    columns = [desc[0] for desc in cursor.description]
    return data, columns


# Contributor: Neha Prakash
# Routine to convert sql rows to dataframe
def convert_rows_to_df(data_rows, cols):
    df = []
    for data in data_rows:
        df.append(pd.DataFrame(data, columns=cols))
    return df
