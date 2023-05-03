import Include
import pandas as pd
from pandas import json_normalize
import json
from sqlalchemy import create_engine
connStringRed = create_engine(Include.setConnRed())
conStringRed2 = Include.setConnRed2()

orders_file = 'stg_order_' + Include.setFileTailRb()
orders_dir = './data/Orders/'
orders_full_path = orders_dir + orders_file

f = open('V2Order.json', )
ord_data = json.load(f)

result = json_normalize(
      ord_data['Records'],
      'OrderLines',
      ['OrderNumber',
       'CustomerNumber',
       'StatusCode',
       'TotalAmount',
       'PartnerPO',
       'OrderDate',
       'DocumentDate',
       ['SalesRequirement']],
      ['ExtendedAttributes']
    ,errors='ignore'
     )



# record_prefix='_'
print(result)
#result.to_csv("try3.csv")
# result.rename(columns={
#                     'Price': 'price',
#                     'RetailPrice': 'retail_price',
#                     'Cost': 'cost',
#                     'MSRP': 'msrp',
#                     'Description': 'product_name',
#                     'Discounts': 'discounts',
#                     'ShipmentInfos': 'shipment_infos',
#                     'Taxes': 'taxes',
#                     'IsDropShip': 'is_dropship',
#                     'Quantity': 'quantity',
#                     'QuantityUOM': 'quantity_uom',
#                     'LineNumber': 'line_number',
#                     'Weight': 'weight',
#                     'ExtendedAttributes': 'extended_attributes',
#                     'ItemIdentifier.SupplierSKU': 'supplier_sku',
#                     'ItemIdentifier.PartnerSKU': 'partner_sku',
#                     'ItemIdentifier.UPC': 'upc',
#                     'OrderNumber': 'order_number',
#                     'CustomerNumber': 'customer_number',
#                     'StatusCode': 'status_code',
#                     'TotalAmount': 'total_amount',
#                     'PartnerPO': 'partner_po',
#                     'OrderDate': 'order_date',
#                     'DocumentDate': 'document_date',
#                     'SalesRequirement': 'sales_requirement'
#                        }, inplace=True)
#
# print(result.columns)
# df = result[['price','retail_price','quantity','line_number','order_number','status_code','document_date','product_name','cost','msrp','is_dropship','order_date','partner_sku','quantity_uom','weight','supplier_sku','upc','customer_number','total_amount','partner_po']]
#df.to_parquet(orders_full_path)
#print(df)

#df2 = pd.read_parquet(orders_dir)
#df2.to_sql('stg_order_new4', connStringRed, index=False, if_exists='append', schema='rb')







# dest_dir = Include.setTargetDir()
# source_dir = Include.setSouceDir()
# s3_upload_file_ord = Include.setS3UploadFileOrders()
# match_cond = Include.setMatchCondOrd()
# for file in glob.glob(source_dir + match_cond):
#     shutil.copy(file, dest_dir)
#
# result2 = subprocess.Popen(s3_upload_file_ord, shell=True)


