import Include
import pandas as pd
import xml.etree.ElementTree as ET
from sqlalchemy import create_engine
connStringRed = create_engine(Include.setConnRed())

etree = ET.parse('SalesOrders.xml')
dfcols = ["LogicbrokerKey", "OrderNumber", "OrderDate", "Status"]
rows = []

for salesorder in etree.iter('SalesOrder'):
    LogicbrokerKey = salesorder.find('LogicbrokerKey').text
    OrderNumber = salesorder.find('OrderNumber').text
    OrderDate = salesorder.find('OrderDate').text
    Status = salesorder.find('Status').text
    rows.append({"LogicbrokerKey": LogicbrokerKey, "OrderNumber": OrderNumber,"OrderDate": OrderDate,"Status": Status})

df = pd.DataFrame(rows,columns=dfcols)
df.to_sql('sales_orders', connStringRed, index=False, if_exists='replace', schema='rb')
print(df)


