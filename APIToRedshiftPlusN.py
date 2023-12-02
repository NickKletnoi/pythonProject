import Include
import pyodbc
import requests
import pandas as pd
from sqlalchemy import create_engine
connStringRed = create_engine(Include.setConnRed())

#'postgresql://reservebar-master:0$sd^e2ivN!9xP!MO4Mr@reservebar-master.cosesp8bmzst.us-west-2.redshift.amazonaws.com:5439/reservebar-master'

conStringRed2 = Include.setConnRed2()
url = Include.setAPIURLNew()
conn = pyodbc.connect(conStringRed2)
#########################################################
API_Subscriptionkey = Include.setAPIsubscriptionkey()
API_SenderCompanyID = Include.setAPIFiltersSenderCompanyID()
API_Filterfrom = Include.setAPIFilterfrom()
API_FilterStatus = Include.setAPIFilterstatus()
print("**Attaching to Orders Endpoint and Processing Response.**")
payload={
   "subscription-key": API_Subscriptionkey,
   "Filters.senderCompanyId": API_SenderCompanyID,
   "Filters.from": API_Filterfrom,
   "Filters.status": API_FilterStatus
 }
response = requests.get(url=url,params=payload)
print(response.url)
print("**Mapping Orders Response to Redshift table.**")
data = response.json()
dfcols = ["LogicbrokerKey", "OrderNumber", "OrderDate", "Status"]
rows = []

for order in data['Body']['SalesOrders']:
        LogicbrokerKey = order['LogicbrokerKey']
        OrderNumber = order['OrderNumber']
        OrderDate = order['OrderDate']
        Status = order['Status']
        rows.append(
            {"LogicbrokerKey": LogicbrokerKey, "OrderNumber": OrderNumber, "OrderDate": OrderDate, "Status": Status})
df = pd.DataFrame(rows,columns=dfcols)
df.to_sql('sales_orders_stage', connStringRed, index=False, if_exists='append', schema='rb')

print("**Upload to Redshift Completed Successfully**")

final_load_query = """CALL rb.usp_final_load();"""
cursor = conn.cursor()
cursor.execute(final_load_query)
cursor.close()
conn.commit()
conn.close()

print("**Final Load on Redshift Completed Successfully**")
