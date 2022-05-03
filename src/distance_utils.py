import pandas as pd
import numpy as np
from scipy.stats import entropy, wasserstein_distance

epsilon = 1e-5


# Given data in the form:
# attribute_type(Ex: female), f1(m1), f1(m2).., f1(m5), f2(m1),..f2(m5)..,f5(m5), married/unmarried
def find_distance(data, cols):
    df = pd.DataFrame(data)

    # Separate married/unmarried rows
    df_married = df[df.iloc[:, -2] == 1]
    df_unmarried = df[df.iloc[:, -2] == 0]

    target_df = pd.DataFrame()
    reference_df = pd.DataFrame()

    distance = {}

    for index in range(1, df.shape[1] - 2):
        married_col = df_married.iloc[:, index]
        unmarried_col = df_unmarried.iloc[:, index]

        target_df["aggregate"] = married_col.values

        reference_df["aggregate"] = unmarried_col.values
        distance[cols[index]] = kl_divergence(target_df, reference_df)

    return distance


# Given the result of executing a query on both tables, find the Earth Mover's distance
def emd_distance(target_rows, reference_rows):
    # Pad to make sizes equal
    m = max(len(target_rows), len(reference_rows))
    target_rows = np.array(target_rows["aggregate"])
    target_rows.resize(m)

    reference_rows = np.array(reference_rows["aggregate"])
    reference_rows.resize(m)

    target_rows = target_rows.tolist()
    reference_rows = reference_rows.tolist()

    # Normalize values
    target_sum = np.sum(target_rows)
    reference_sum = np.sum(reference_rows)

    target_rows = target_rows / (target_sum + epsilon)
    reference_rows = reference_rows / (reference_sum + epsilon)

    # To prevent divide by zero errors
    target_rows[np.where(target_rows < epsilon)] = epsilon
    reference_rows[np.where(reference_rows < epsilon)] = epsilon

    return wasserstein_distance(target_rows, reference_rows)


# Given the result of executing a query on both tables, find the KL divergence
def kl_divergence(target_rows, reference_rows):
    # Pad to make sizes equal
    m = max(len(target_rows), len(reference_rows))
    target_rows = np.array(target_rows["aggregate"])
    target_rows.resize(m)

    reference_rows = np.array(reference_rows["aggregate"])
    reference_rows.resize(m)

    target_rows = target_rows.tolist()
    reference_rows = reference_rows.tolist()

    # Normalize values
    target_sum = np.sum(target_rows)
    reference_sum = np.sum(reference_rows)

    target_rows = target_rows / (target_sum + epsilon)
    reference_rows = reference_rows / (reference_sum + epsilon)

    # To prevent divide by zero errors
    target_rows[np.where(target_rows < epsilon)] = epsilon
    reference_rows[np.where(reference_rows < epsilon)] = epsilon

    return entropy(target_rows, reference_rows)
