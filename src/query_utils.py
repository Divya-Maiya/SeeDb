
def generate_aggregate_queries(A, M, F, table):
    # A - Dimension attributes (group by), M - Measure attribute (aggregate), F - Aggregate functions
    print("Generating aggregate queries")

    queries = []
    for a in A:
        for m in M:
            for f in F:
                queries.append("SELECT {}, {}({}) FROM {} GROUP BY {}".format(a, f, m, table, a))

    return queries