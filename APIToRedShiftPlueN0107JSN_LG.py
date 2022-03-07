import Include
import pandas as pd
from pandas import json_normalize
import sys
import requests
from sqlalchemy import create_engine
connStringRed = create_engine(Include.setConnRed())
conStringRed2 = Include.setConnRed2()

url = Include.setAPIURLNewV2()
print("**Attaching to Orders Endpoint and Processing Response.**")
API_Subscriptionkey = Include.setAPIsubscriptionkey()
API_SenderCompanyID = Include.setAPIFiltersSenderCompanyID()
API_Filterfrom = Include.setAPIFilterfromNew()
API_FilterTo = Include.setAPIFilterToNew()
API_FilterStatus = Include.setAPIFilterstatus()
payload = Include.setAPIPayLoad()
fileTail = Include.setFileTailRb()
fileTailJ = Include.setFileTailRbJ()
RBDir = Include.setReserveBarDir()
final_file = RBDir + 'Orders_' + fileTail
final_fileJ = RBDir + 'Orders_' + fileTailJ

response = requests.get(url=url,params=payload)

print("**Mapping Orders Response to Redshift table and processing pages... please standby...**")
print(response.url)
url1 = response.url
data = response.json()

Totalpages = int(data['TotalPages'])
TotalRecords = int(data['TotalRecords'])
page = 0

toolbar_width = 40
sys.stdout.write("[%s]" % (" " * toolbar_width))
sys.stdout.flush()
sys.stdout.write("\b" * (toolbar_width+1))

while page < Totalpages:
    orders_file = 'stg_order_' + str(page) + '_' + Include.setFileTailRb()
    orders_dir = './data/Orders/'
    orders_full_path = orders_dir + orders_file

    payload2 = {
        "subscription-key": API_Subscriptionkey,
        "Filters.senderCompanyId": API_SenderCompanyID,
        "Filters.from": API_Filterfrom,
        "Filters.to": API_FilterTo,
        "Filters.status": API_FilterStatus,
        "Filters.pageSize": '70',
        "Filters.page": page
    }

    response = requests.get(url=url, params=payload2)
    data = response.json()


    result = json_normalize(
          data['Records'],
          'OrderLines',
          ['OrderNumber',
           'CustomerNumber',
           'StatusCode',
           'TotalAmount',
           'PartnerPO',
           'OrderDate',
           'DocumentDate',
           ['SalesRequirement']], errors='ignore'
         )

    result.rename(columns={
                        'Price': 'price',
                        'RetailPrice': 'retail_price',
                        'Cost': 'cost',
                        'MSRP': 'msrp',
                        'Description': 'product_name',
                        'Discounts': 'discounts',
                        'ShipmentInfos': 'shipment_infos',
                        'Taxes': 'taxes',
                        'IsDropShip': 'is_dropship',
                        'Quantity': 'quantity',
                        'QuantityUOM': 'quantity_uom',
                        'LineNumber': 'line_number',
                        'Weight': 'weight',
                        'ExtendedAttributes': 'extended_attributes',
                        'ItemIdentifier.SupplierSKU': 'supplier_sku',
                        'ItemIdentifier.PartnerSKU': 'partner_sku',
                        'ItemIdentifier.UPC': 'upc',
                        'OrderNumber': 'order_number',
                        'CustomerNumber': 'customer_number',
                        'StatusCode': 'status_code',
                        'TotalAmount': 'total_amount',
                        'PartnerPO': 'partner_po',
                        'OrderDate': 'order_date',
                        'DocumentDate': 'document_date',
                        'SalesRequirement': 'sales_requirement'
                           }, inplace=True)

    #print(result.columns)
    df = result[['price','retail_price','quantity','line_number','order_number','status_code','document_date','product_name','cost','msrp','is_dropship','order_date','partner_sku','quantity_uom','weight','supplier_sku','upc','customer_number','total_amount','partner_po']]
    df.to_parquet(orders_full_path)

    sys.stdout.write("#")
    sys.stdout.flush()

    page += 1
sys.stdout.write("]\n")

orders_dir = './data/Orders/'
print("**Transfering Data to Redshift please standby....**")
df2 = pd.read_parquet(orders_dir)
df2.to_sql('stg_order_new', connStringRed, index=False, if_exists='append', schema='rb')

print("**Upload to Redshift Completed Successfully with total of %s API Pages and %s orders processed in current Payload **" % (Totalpages,TotalRecords))

