# Contributors: Chirag Uday Kamath (cukamath@umass.edu), Divya Maiya (dmaiya@umass.edu)
import pandas as pd
import numpy as np
from scipy.stats import entropy, wasserstein_distance
import math

epsilon = 1e-5


# Given data in the form:
# attribute_type(Ex: female), f1(m1), f1(m2).., f1(m5), f2(m1),..f2(m5)..,f5(m5), married/unmarried
# Contributor: Chirag Uday Kamath
def find_distance(data, cols, measure):
    df = pd.DataFrame(data)

    # Separate target and reference
    df_target = df[df.iloc[:, -2] == 1]
    df_reference = df[df.iloc[:, -2] == 0]

    target_df = pd.DataFrame()
    reference_df = pd.DataFrame()

    distance = {}

    for index in range(1, df.shape[1] - 2):
        married_col = df_target.iloc[:, index]
        unmarried_col = df_reference.iloc[:, index]

        target_df["aggregate"] = married_col.values

        reference_df["aggregate"] = unmarried_col.values
        distance[cols[index]] = calculate_distance(measure, target_df, reference_df)

    return distance


# Contributor: Chirag Uday Kamath
def calculate_distance(measure, target_df, reference_df):
    if measure == 'kl_divergence':
        return kl_divergence(target_df, reference_df)
    elif measure == 'emd_distance':
        return emd_distance(target_df, reference_df)
    elif measure == 'js_divergence_distance':
        return js_divergence_distance(target_df, reference_df)
    else:
        return euclidean_distance(target_df, reference_df)


# Contributor: Divya Maiya
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


# Contributor: Divya Maiya
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


# Contributor: Divya Maiya
# Given the result of executing a query on both tables, find the JS Divergence distance
def js_divergence_distance(target_rows, reference_rows):
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

    # Create Averaged list for js divergence
    averaged_rows = (target_rows + reference_rows) / 2

    # To prevent divide by zero errors
    target_rows[np.where(target_rows < epsilon)] = epsilon
    reference_rows[np.where(reference_rows < epsilon)] = epsilon
    averaged_rows[np.where(averaged_rows < epsilon)] = epsilon

    return (entropy(target_rows, averaged_rows) + entropy(reference_rows, averaged_rows)) / 2


# Contributor: Divya Maiya
# Given the result of executing a query on both tables, find the euclidean distance divergence
def euclidean_distance(target_rows, reference_rows):
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

    return math.sqrt(np.sum((target_rows - reference_rows) ** 2))
