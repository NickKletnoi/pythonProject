import config1 as cfg
import pandas as pd
import pyodbc as py

# conn = cfg.DATABASE_CONFIG['conn_str']
# cntx = py.connect(conn)

con = cfg.Connection()

df = pd.read_sql("select * from contacts",con)
for i,(index, row) in enumerate(df.iterrows()):
        if i == 0:
                last_name = row["last_name"]
                first_name = row["first_name"]

print(first_name)
print(last_name)




