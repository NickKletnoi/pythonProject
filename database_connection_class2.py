import config1 as cfg
import pandas as pd
import pyodbc as py

cntx = py.connect(cfg.DATABASE_CONFIG['conn_str'])

query=('''
select * from contacts where phones=? and first_name = ?
''')
phone_val = '(408)-123-3456,(408)-123-3457'
name_val = 'John'

df = pd.read_sql(query,cntx,params=[phone_val,name_val])
for i,row in df.iterrows():
        if i == 0:
                last_name = row["last_name"].strip()
                first_name = row["first_name"].strip()

print(first_name)
print(last_name)




