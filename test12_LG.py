import Include
import subprocess
import pandas as pd
from pandas import json_normalize
import glob
import shutil
import requests
from sqlalchemy import create_engine
connStringRed = create_engine(Include.setConnRed())
conStringRed2 = Include.setConnRed2()

url = Include.setAPIURLNewV2()
API_Subscriptionkey = Include.setAPIsubscriptionkey()
API_SenderCompanyID = Include.setAPIFiltersSenderCompanyID()
API_Filterfrom = Include.setAPIFilterfromNew()
API_FilterTo = Include.setAPIFilterToNew()
API_FilterStatus = Include.setAPIFilterstatus()
payload = Include.setAPIPayLoad()
fileTail = Include.setFileTailRb()
RBDir = Include.setReserveBarDir()
final_file = RBDir + 'Orders_' + fileTail

response = requests.get(url=url,params=payload)
print("**Attaching to Orders Endpoint and Processing Response.**")
print(response.url)
print("**Processing Orders API and Uploading to Redshift and S3 ...")
data = response.json()

orders_file = 'stg_order_' + Include.setFileTailRb()
orders_dir = './data/Orders/'
orders_full_path = orders_dir + orders_file

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


df = result[['price','retail_price','quantity','line_number','order_number','status_code','document_date','product_name','cost','msrp','is_dropship','order_date','partner_sku','quantity_uom','weight','supplier_sku','upc','customer_number','total_amount','partner_po']]
df.to_parquet(orders_full_path)

df2 = pd.read_parquet(orders_dir)
df2.to_sql('stg_order_new', connStringRed, index=False, if_exists='append', schema='rb')

dest_dir = Include.setTargetDir()
source_dir = Include.setSouceDir()
s3_upload_file_ord = Include.setS3UploadFileOrders()
match_cond = Include.setMatchCondOrd()
for file in glob.glob(source_dir + match_cond):
    shutil.copy(file, dest_dir)

result2 = subprocess.Popen(s3_upload_file_ord, shell=True)


