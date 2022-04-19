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


# Given the result of executing a query on both tables (married/unmarried), find the KL divergence
# Example Queries:
# target: select sex, avg(capital_gain) from census where marital_status LIKE '%Never-married%' group by sex;
# ref: select sex, avg(capital_gain) from census where marital_status LIKE '%Married%' group by sex;
def kl_divergence(data, cols):
    df = convert_rows_to_df(data, cols)
    target_df = df[0]
    reference_df = df[1]

    # for now this will compare queries with only 1 aggregate attribute (ex: avg)
    # TODO: extension to include multiple aggregate attributes in a single query
    # TODO: Manually check if the value for KL Divergence is right
    target_cols = []
    for col in ["sum", "min", "max", "avg", "count"]:
        if col in target_df:
            target_cols.append(col)

    for col in target_cols:
        target_vals = target_df[col]
        reference_vals = reference_df[col]

        # Value of the aggregate function from both the target and reference views
        tgt = np.array(target_vals)
        ref = np.array(reference_vals)

        # Normalize value of the aggregate function to get a probability distribution
        tgt_prob = tgt / np.sum(tgt)
        ref_prob = ref / np.sum(ref)

        tgt_prob = tgt_prob.astype(float)
        ref_prob = ref_prob.astype(float)

        print(tgt_prob)
        # sum = 0
        # for qi, pi in zip(tgt_prob, ref_prob):
        #     print(pi)
        #     print(qi)
        #     print(pi / qi)
        #     # pi.float()
        #     # qi.float()
        #     sum += qi * np.log(pi / qi)
        # return sum

        # Apply the formula for KL Divergence
        # Refer https://en.wikipedia.org/wiki/Kullback%E2%80%93Leibler_divergence
        return np.sum([qi * np.log(pi / qi) for qi, pi in zip(tgt_prob, ref_prob) if pi > 0 and qi > 0])


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
