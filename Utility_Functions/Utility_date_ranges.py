#
# 0 - Create utility function
#

# required library
import pandas as pd


# define function
def expand_date_range_to_list(start_dte, end_dte):
    return pd.date_range(start=start_dte, end=end_dte).strftime("%Y-%m-%d").tolist()


# required libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *

# register df function
udf_expand_date_range_to_list = udf(expand_date_range_to_list, ArrayType(StringType()))

# register sql function
spark.udf.register("sql_expand_date_range_to_list", udf_expand_date_range_to_list)


# test function
out = expand_date_range_to_list("2022-09-01", "2022-09-05")
out