import Include
import pandas as pd
import json
#from sqlalchemy import create_engine
#connStringRed = create_engine(Include.setConnRed())

dfcols = ["OrderNumber"]
rows = []


with open('V2Order1.json') as json_file:
    data = json.load(json_file)

    for order in data['Records']:
        OrderNumber = order['OrderNumber']
        rows.append(
            {"OrderNumber": OrderNumber})
df = pd.DataFrame(rows,columns=dfcols)
#df.to_sql('sales_orders2', connStringRed, index=False, if_exists='replace', schema='rb')
print(df)





