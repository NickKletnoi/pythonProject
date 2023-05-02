# import pyspark.sql.functions as F
# from pyspark.sql.types import *
import pprint
import pandas as pd



df = pd.read_csv('data/sample3.csv')

def checkMinimumValueColumns_fn(dataframe, column_map):
    """ Only supported for numerical columns (double, integer, long, short)
    {'account_id' : 2,'process_date':2,'quantity':2}
    0 means success and 1 means failure of the rules
    """
    key_list = list(map(lambda l: l[0], column_map.items()))
    typesMap = {f for f in dataframe.dtypes.items()}
    dq_map = dict(filter(lambda l : l[1] == 'integer' or l[1] == 'double' or l[1] == 'long' or l[1] == 'short' or l[1] == 'decimal' or l[1] == 'float', typesMap.items()))
    #column_map = {x:column_map[x] for x in column_map if x in dq_map}
    #minimum_value_list = [ F.when(F.min(item[0]) >= F.lit(item[1]),0).otherwise(1).alias(item[0]) for item in list(map(lambda l: l, column_map.items()))]
    # = dataframe.select(*minimum_value_list).collect()[0].asDict()
    return dq_map

col_map = {'account_id' : 3,'quantity':7}
res = checkMinimumValueColumns_fn(df,col_map)

pprint.pprint(res, width=1)
print(df)