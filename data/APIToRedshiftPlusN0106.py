import Include
import pyodbc
import requests
import pandas as pd
import json
from sqlalchemy import create_engine
connStringRed = create_engine(Include.setConnRed())

conStringRed2 = Include.setConnRed2()
url = Include.setAPIURLNewV2()
conn = pyodbc.connect(conStringRed2)
print("**Attaching to Orders Endpoint and Processing Response.**")
API_Subscriptionkey = Include.setAPIsubscriptionkey()
API_SenderCompanyID = Include.setAPIFiltersSenderCompanyID()
API_Filterfrom = Include.setAPIFilterfromNew()
API_FilterTo = Include.setAPIFilterToNew()
API_FilterStatus = Include.setAPIFilterstatus()

payload={
   "subscription-key": API_Subscriptionkey,
   "Filters.senderCompanyId": API_SenderCompanyID,
   "Filters.from": API_Filterfrom,
   "Filters.to": API_FilterTo,
   "Filters.status": API_FilterStatus
 }
response = requests.get(url=url,params=payload)
print(response.url)
print("**Mapping Orders Response to Redshift table.**")
data = response.json()
# r2 = json.dumps(data, indent=4)
# print(r2)


dfcols = [
    "retailer_id",
    "order_number",
    "order_date",
    "document_date",
    "supplier_sku",
    "partner_sku",
    "upc",
    "price",
    "product_name",
    "quantity",
    "line_number",
    "vendor",
    "status"
 ]

rows = []

for order in data['Records']:

        retailer_id = order['ReceiverCompanyId']
        order_number = order['OrderNumber']
        order_date = order['OrderDate']
        document_date = order['DocumentDate']
        supplier_sku = order['OrderLines'][0]['ItemIdentifier']['SupplierSKU']
        partner_sku = order['OrderLines'][0]['ItemIdentifier']['PartnerSKU']
        upc = order['OrderLines'][0]['ItemIdentifier']['UPC']
        price = order['OrderLines'][0]['Price']
        product_name = order['OrderLines'][0]['Description']
        quantity = order['OrderLines'][0]['Quantity']
        line_number = order['OrderLines'][0]['LineNumber']
        vendor = order['OrderLines'][0]['ExtendedAttributes'][0]['Value']
        status = order['StatusCode']
        print(order_number)
        rows.append(
            {
                "retailer_id": retailer_id,
                "order_number": order_number,
                "order_date": order_date,
                "document_date": document_date,
                "supplier_sku": supplier_sku,
                "partner_sku": partner_sku,
                "upc": upc,
                "price": price,
                "product_name": product_name,
                "quantity": quantity,
                "line_number": line_number,
                "vendor": vendor,
                "status": status
            })


df = pd.DataFrame(rows,columns=dfcols)
df.to_sql('orders_new3', connStringRed, index=False, if_exists='append', schema='rb')

print("**Upload to Redshift Completed Successfully**")

#final_load_query = """CALL rb.usp_final_load();"""
#cursor = conn.cursor()
#cursor.execute(final_load_query)
#cursor.close()
conn.commit()
conn.close()

print("**Final Load on Redshift Completed Successfully**")
