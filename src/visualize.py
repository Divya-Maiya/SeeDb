import numpy as np
import matplotlib.pyplot as plt
import query_generator


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
