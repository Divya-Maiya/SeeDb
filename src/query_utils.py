import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


def generate_aggregate_queries(A, M, F, table):
    # A - Dimension attributes (group by), M - Measure attribute (aggregate), F - Aggregate functions
    print("Generating aggregate queries")

    queries = []
    for a in A:
        for m in M:
            for f in F:
                queries.append("SELECT {}, {}({}) FROM {} GROUP BY {}".format(a, f, m, table, a))

    return queries


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


def execute_queries(cursor, queries):
    data = []
    for query in queries:
        cursor.execute(query)
        data.append(cursor.fetchall())

    columns = [desc[0] for desc in cursor.description]
    return data, columns


def convert_rows_to_df(data_rows, cols):
    df = []
    for data in data_rows:
        df.append(pd.DataFrame(data, columns=cols))
    return df


# Given data in the form:
# attribute_type(Ex: female), f1(m1), f1(m2).., f1(m5), f2(m1),..f2(m5)..,f5(m5), married/unmarried
# Output:
def transform_data(data):
    # Example data
    # workclass A, sum(age), min(age), max(age),.., sum(capital_gain)..
    # Convert to database
    df = pd.DataFrame(data)

    # Separate married/unmarried rows
    df_married = df[df.iloc[:, -2] == 1]
    df_unmarried = df[df.iloc[:, -2] == 0]

    # print(df_married.head())
    # print("------------------------------------")
    # print(df_unmarried.head())
    # print("------------------------------------")
    # Get all rows for each attribute

    target_df = pd.DataFrame()
    reference_df = pd.DataFrame()
    # print(df[0])

    target_df["attributes"] = df_married[0]
    reference_df["attributes"] = df_unmarried[0]
    # print(target_df)
    # print(len(target_df["attributes"]))
    # result["attributes"] = df[:, 0]

    # print(result.head())
    #
    distance = []
    # print(df.shape[1])
    #
    for index in range(1, df.shape[1]-2):
        married_col = df_married.iloc[:, index]
        unmarried_col = df_unmarried.iloc[:, index]
        # print(len(married_col.values))
        target_df["aggregate"] = married_col.values

        reference_df["aggregate"] = unmarried_col.values
        # print(target_df.head())
        distance.append(kl_divergence(target_df, reference_df))

    print("--------------")
    print(distance)
    # for index in range(1, df_unmarried.shape[1]-1):
    #     column_obj = df_unmarried.iloc[:, index]
    #     result["unmarried"]["aggregate"] = column_obj.values



    # Sample output: attribute_rows
    # [
    #   [workclass A, sum(age), min(age)..., 0]
    #   [workclass B, sum(age), min(age)..., 0]
    #    ...
    #   [workclass z, sum(age), min(age)..., 0]
    # ]



    # KL Divergence needs data in the form
    # [[attribute, fi(mj)][attribute, fi(mj)]] - for married and unmarried


# Given the result of executing a query on both tables (married/unmarried), find the KL divergence
# Example Queries:
# target: select sex, avg(capital_gain) from census where marital_status LIKE '%Never-married%' group by sex;
# ref: select sex, avg(capital_gain) from census where marital_status LIKE '%Married%' group by sex;
def kl_divergence(target_df, reference_df):
    # df = convert_rows_to_df(data, cols)
    # target_df = df["married"]
    # reference_df = df["unmarried"]
    print(target_df.head())
    # for now this will compare queries with only 1 aggregate attribute (ex: avg)
    # TODO: extension to include multiple aggregate attributes in a single query
    # target_cols = []
    # for col in ["sum", "min", "max", "avg", "count"]:
    #     if col in target_df:
    #         target_cols.append(col)

    target_vals = target_df["aggregate"]
    reference_vals = reference_df["aggregate"]

    tgt = np.array(target_vals)
    ref = np.array(reference_vals)

    if np.sum(tgt) != 0:
        tgt_prob = tgt / np.sum(tgt)
        ref_prob = ref / np.sum(ref)

        tgt_prob = tgt_prob.astype(float)
        ref_prob = ref_prob.astype(float)

        return np.sum([pi * np.log(pi / qi) for pi, qi in zip(tgt_prob, ref_prob) if pi > 0 and qi > 0])
    else:
        return 0


    # for col in target_cols:
    #     target_vals = target_df[col]
    #     reference_vals = reference_df[col]
    #
    #     # Value of the aggregate function from both the target and reference views
    #     tgt = np.array(target_vals)
    #     ref = np.array(reference_vals)
    #
    #     # Normalize value of the aggregate function to get a probability distribution
    #     tgt_prob = tgt / np.sum(tgt)
    #     ref_prob = ref / np.sum(ref)
    #
    #     tgt_prob = tgt_prob.astype(float)
    #     ref_prob = ref_prob.astype(float)
    #
    #     print(tgt_prob)
    #     # sum = 0
    #     # for qi, pi in zip(tgt_prob, ref_prob):
    #     #     print(pi)
    #     #     print(qi)
    #     #     print(pi / qi)
    #     #     # pi.float()
    #     #     # qi.float()
    #     #     sum += qi * np.log(pi / qi)
    #     # return sum
    #
    #     # Apply the formula for KL Divergence
    #     # Refer https://en.wikipedia.org/wiki/Kullback%E2%80%93Leibler_divergence
    #     # pi refers to probabilities concerning the target and qi references that of reference.
    #     return np.sum([pi * np.log(pi / qi) for pi, qi in zip(tgt_prob, ref_prob) if pi > 0 and qi > 0])


# Bar charts for married/unmarried views Data and col values as obtained from executing the both target and reference
# queries should be the input for this function
# Similar to Fig 1 in the Reference paper
def visualize_data(data, cols, title="Average Capital Gain Group by Sex"):
    # % matplotlib inline
    plt.style.use('ggplot')
    N = 2

    aggregate = cols[1]
    df = convert_rows_to_df(data, cols)

    unmarried = df[0][aggregate]
    married = df[1][aggregate]

    ind = np.arange(N)
    width = 0.35
    plt.bar(ind, unmarried, width, label='Unmarried')
    plt.bar(ind + width, married, width,
            label='Married')

    plt.ylabel(aggregate)
    plt.xlabel('Sex')
    plt.title(title)

    plt.xticks(ind + width / 2, ('Female', 'Male'))
    plt.legend(loc='best')
    plt.show()
