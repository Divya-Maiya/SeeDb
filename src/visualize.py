# Contributor: Chirag Uday Kamath

import numpy as np
import matplotlib.pyplot as plt
from requests import head
import query_generator
import itertools
import termtables as tt


# Visualize census data
def visualize_census_data(cursor, a, f, m):
    print("Visualizing {} v/s {}({})".format(a, f, m))

    # Get married and unmarried
    cursor.execute(query_generator.get_married_data(a, f, m))
    married_views = cursor.fetchall()

    cursor.execute(query_generator.get_unmarried_data(a, f, m))
    unmarried_views = cursor.fetchall()

    # Get all keys
    order_keys = set()
    for pair in married_views:
        order_keys.add(pair[0].strip())
    for pair in unmarried_views:
        order_keys.add(pair[0].strip())

    # Get vals for married and unmarried
    married_dict = {}
    unmarried_dict = {}

    for pair in married_views:
        married_dict[pair[0].strip()] = pair[1]

    for pair in unmarried_views:
        unmarried_dict[pair[0].strip()] = pair[1]

    married_vals = []
    unmarried_vals = []

    # Ensure 0 padding for ordered data
    for key in order_keys:
        if key in married_dict:
            married_vals.append(float(married_dict[key]))
        else:
            married_vals.append(float(0))

        if key in unmarried_dict:
            unmarried_vals.append(float(unmarried_dict[key]))
        else:
            unmarried_vals.append(float(0))

    # Plot the data
    x_axis = np.arange(len(list(order_keys)))

    plt.bar(x_axis - 0.2, married_vals, 0.4, label='Married')
    plt.bar(x_axis + 0.2, unmarried_vals, 0.4, label='Unmarried')

    plt.xticks(x_axis, order_keys, rotation='vertical')
    plt.xlabel(a)
    plt.ylabel(f + "(" + m + ")")

    plt.title(a + " vs " + f + "(" + m + ")")
    plt.legend()

    plt.show()


# Visualize dblp data
def visualize_dblp_data(cursor, a, f, m):
    print("Visualizing {} v/s {}({})".format(a, f, m))

    # Get target and reference
    cursor.execute(query_generator.get_type0_data(a, f, m))
    ref_views = cursor.fetchall()

    cursor.execute(query_generator.get_type13_data(a, f, m))
    target_views = cursor.fetchall()

    # Get all keys
    order_keys = set()
    for pair in ref_views:
        order_keys.add(pair[0].strip())
    for pair in target_views:
        order_keys.add(pair[0].strip())

    # Get vals for type 0 and type 1/3
    ref_dict = {}
    target_dict = {}

    for pair in ref_views:
        ref_dict[pair[0].strip()] = pair[1]

    for pair in target_views:
        target_dict[pair[0].strip()] = pair[1]

    ref_vals = []
    target_vals = []

    # Ensure 0 padding for ordered data
    for key in order_keys:
        if key in ref_dict:
            ref_vals.append(float(ref_dict[key]))
        else:
            ref_vals.append(float(0))

        if key in target_dict:
            target_vals.append(float(target_dict[key]))
        else:
            target_vals.append(float(0))

    # Plot the data
    x_axis = np.arange(len(list(order_keys)))

    plt.bar(x_axis - 0.2, ref_vals, 0.4, label='Type 1,3')
    plt.bar(x_axis + 0.2, target_vals, 0.4, label='Type 0')

    plt.xticks(x_axis, order_keys, rotation='vertical')
    plt.xlabel(a)
    plt.ylabel(f + "(" + m + ")")

    plt.title(a + " vs " + f + "(" + m + ")")
    plt.legend()

    plt.show()


def visualise_latency_plots(total_runtime, sharing_runtime, pruning_runtime, phases):
    # Usage example:
    # set width of bars
    barWidth = 0.25

    # set heights of bars
    bars1 = []
    bars2 = []
    bars3 = []
    bars1.extend(sharing_runtime)
    bars2.extend(pruning_runtime)
    sharing_total = pruning_total = 0
    for i in range(len(bars1)):
        sharing_total += bars1[i]
        pruning_total += bars2[i]
    bars1.append(sharing_total)
    bars2.append(pruning_total)
    for i in range(len(bars1) - 1):
        bars3.append(bars1[i] + bars2[i])
    bars3.extend(total_runtime)
    x_axis = []
    for i in range(1, phases + 1):
        x_axis.append('phase' + str(i))
    x_axis.append('total_time')

    row1 = ['Sharing Based']
    row1.extend(bars1)
    row2 = ['Pruning Based']
    row2.extend(bars2)
    row3 = ['Total Runtime']
    row3.extend(bars3)
    header = ['Operations']
    header.extend(x_axis)
    latency_table = tt.to_string(
        [row1, row2, row3],
        header=header,
        style=tt.styles.ascii_thin_double,
    )
    print('LATENCY TABLE :')
    print(latency_table)

    r1 = np.arange(len(bars1))
    r2 = [x + barWidth for x in r1]
    r3 = [x + barWidth for x in r2]

    plt.bar(r1, bars1, color='r', width=barWidth, edgecolor='white', label='Sharing Based')
    plt.bar(r2, bars2, color='g', width=barWidth, edgecolor='white', label='Pruning Based')
    plt.bar(r3, bars3, color='b', width=barWidth, edgecolor='white', label='Total Runtime')

    plt.xlabel('group', fontweight='bold')

    plt.xticks([r + barWidth for r in range(len(bars1))], x_axis)
    plt.xlabel("Phase")
    plt.ylabel("Run time")
    plt.legend()

    plt.show()
