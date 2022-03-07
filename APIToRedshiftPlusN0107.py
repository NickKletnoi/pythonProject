import Include
import pyodbc
import requests
import pandas as pd
import sys
import time
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
payload = Include.setAPIPayLoad()

response = requests.get(url=url,params=payload)
print(response.url)
print("**Mapping Orders Response to Redshift table and processing pages... please standby...**")
data = response.json()
Totalpages = data['TotalPages']
TotalRecords = data['TotalRecords']

dfcols = [
    "retailer_id",
    "order_number",
    "order_date",
    "document_date",
    "supplier_sku",
    "upc",
    "price",
    "product_name",
    "quantity",
    "line_number",
    "vendor",
    "status",
    "logic_broker_key",
    "po_number",
    "state"
 ]

rows = []
page = 1

toolbar_width = 40
sys.stdout.write("[%s]" % (" " * toolbar_width))
sys.stdout.flush()
sys.stdout.write("\b" * (toolbar_width+1))

while page <= Totalpages:
    payload = {
        "subscription-key": API_Subscriptionkey,
        "Filters.senderCompanyId": API_SenderCompanyID,
        "Filters.from": API_Filterfrom,
        "Filters.to": API_FilterTo,
        "Filters.status": API_FilterStatus,
        'page': page
    }

    response = requests.get(url=url, params=payload)

    data = response.json()

    for order in data['Records']:
            time.sleep(0.1)
            retailer_id = order['ReceiverCompanyId']
            order_number = order['OrderNumber']
            order_date = order['OrderDate']
            document_date = order['DocumentDate']
            supplier_sku = order['OrderLines'][0]['ItemIdentifier']['SupplierSKU']
            upc = order['OrderLines'][0]['ItemIdentifier']['UPC']
            price = order['OrderLines'][0]['Price']
            product_name = order['OrderLines'][0]['Description']
            quantity = order['OrderLines'][0]['Quantity']
            line_number = order['OrderLines'][0]['LineNumber']
            vendor = order['OrderLines'][0]['ExtendedAttributes'][0]['Value']
            status = order['StatusCode']
            logic_broker_key = order['Identifier']['LogicbrokerKey']
            po_number = order['PartnerPO']
            state = order['ShipToAddress']['State']
            rows.append(
                {
                    "retailer_id": retailer_id,
                    "order_number": order_number,
                    "order_date": order_date,
                    "document_date": document_date,
                    "supplier_sku": supplier_sku,
                    "upc": upc,
                    "price": price,
                    "product_name": product_name,
                    "quantity": quantity,
                    "line_number": line_number,
                    "vendor": vendor,
                    "status": status,
                    "logic_broker_key": logic_broker_key,
                    "po_number": po_number,
                    "state": state
                })
    sys.stdout.write("#")
    sys.stdout.flush()

    page += 1

sys.stdout.write("]\n")
df = pd.DataFrame(rows,columns=dfcols)
df.to_sql('stg_order', connStringRed, index=False, if_exists='append', schema='rb')

conn.commit()
conn.close()

print("**Upload to Redshift Completed Successfully with total of %s API Pages processed in current Payload **" % (Totalpages))


