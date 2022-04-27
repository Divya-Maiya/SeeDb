def get_split_view_query(i):
    return """
           drop table if exists split_view{};
           create table split_view{} (age real, workclass text, fnlwgt real, education text, education_num real, marital_status text, occupation text, relationship text, race text, sex text, capital_gain real, capital_loss real, hours_per_week real, native_country text, salary text);
           """.format(i, i, i, i)


def get_married_umarried_view_generator_query(i):
    return """
           drop table if exists split_married_{};
           drop table if exists split_unmarried_{};
           create table split_married_{} as (select * from split_view{} where marital_status in (' Married-AF-spouse', ' Married-civ-spouse', ' Married-spouse-absent',' Separated'));
           create table split_unmarried_{} as (select * from split_view{} where marital_status in (' Never-married', ' Widowed',' Divorced'));
           """.format(i, i, i, i, i, i)


def get_target_reference_merged_query(a, query_params, phase):
    return """
           select {}, {},
           case marital_status
           when ' Married-civ-spouse' then 1
           when ' Married-spouse-absent' then 1
           when ' Married-AF-spouse' then 1
           when ' Separated' then 1
           else 0
           end as g1, 1 AS g2
           from split_view{} 
           where not {}=' ?' 
           group by {}, g1, g2
           order by {}
           """.format(a, query_params, phase + 1, a, a, a)

def get_married_data(a,f,m):
    return """
           select {}, {}({})
           from census c
           where c.marital_status in (' Married-AF-spouse', ' Married-civ-spouse', ' Married-spouse-absent',' Separated') and c.{} != ' ?'
           group by {}
           """.format(a,f,m,a, a)

def get_unmarried_data(a,f,m):
    return """
           select {}, {}({})
           from census c
           where c.marital_status in (' Never-married', ' Widowed',' Divorced') and c.{} != ' ?'
           group by {}
           """.format(a,f,m,a, a)