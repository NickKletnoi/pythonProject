from deltalake.writer import write_deltalake
import pandas as pd
from deltalake import DeltaTable

#https://medium.com/plumbersofdatascience/delta-lake-with-python-delta-rs-1ec8d8ebfa27
## writing to delta table
df = pd.DataFrame({"x": [1, 2, 3],"y": [3, 2, 1]})
# reanming the DataFrame columns
df.rename(columns = {'x':'first_col',
                       'y':'second_col'},
            inplace = True)

write_deltalake('./data/mytable', df)

## reading from delta-table
dt = DeltaTable("./data/mytable")
#pdf = dt.to_pandas()

## appending records
df = pd.DataFrame({"first_col": [5],"second_col": [6]})
write_deltalake(dt, df, mode="append")

pdf = dt.to_pandas()
print(pdf)

## getting schema
dt = DeltaTable("./data/mytable")
print(dt.schema().json())
print(dt.files())
print(dt.version())

## to load a specific version of the data
dt = DeltaTable("./data/mytable")
dt.load_version(0)
pdf = dt.to_pandas()
print(pdf)