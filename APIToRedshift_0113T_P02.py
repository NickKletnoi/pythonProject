import Include
import pyodbc
import requests
import pandas as pd
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
payload = {
        "subscription-key": Include.setAPIsubscriptionkey(),
        "Filters.senderCompanyId": Include.setAPIFiltersSenderCompanyID(),
        "Filters.from": Include.setAPIFilterfromNew(),
        "Filters.to": Include.setAPIFilterToNew(),
        "Filters.status": Include.setAPIFilterstatus()
    }
response = requests.get(url=url,params=payload)
print(response.url)
print("**Mapping Orders Response to Redshift table and processing pages... please standby...**")
data = response.json()

# json_object = json.dumps(data, indent=4)
# # Writing to ELK_Payload_SOE-AUDIT.json
# with open(final_file, "w") as outfile:
#     outfile.write(json_object)

Totalpages = data['TotalPages']
TotalRecords = data['TotalRecords']

dfcols = [
              "order_number",
              "order_date",
              "document_date",
              "line_number",
              "vendor",
              "variant_title",
              "engraved",
              "extended_category",
              "alcohol_category",
              "is_alcohol",
              "luxury_draw_string",
              "partner_line_id",
              "customer_number",
              "status_code",
              "partner_po",
              "total_amount",
              "price",
              "upc",
              "sku",
              "cost",
              "msrp",
              "weight",
              "quantity",
              "is_drop_ship",
              "extended_description",
              "pay_in_number_of_days",
              "discount_in_number_of_days",
              "available_discount",
              "hand_made_leather_bottle_holder"
    ]



def parse_single_order_line_v1(order_line):
    """
    Converts a single order line to a dict containing the relevant fields.
    This implementation is more efficient when extended attributes is large
    and the number of queried attributes is a relatively low percentage.
    """

    def find_in_extended_attribute(attribute_name):
        return next(entry["Value"] for entry in order_line["ExtendedAttributes"] if entry["Name"] == attribute_name)

    return {
        "Qty": order_line["Quantity"],
        "SKU": order_line["ItemIdentifier"]["SupplierSKU"],
        "UPC": order_line["ItemIdentifier"]["UPC"],
        "Price": order_line["Price"],
        "Cost": order_line["Cost"],
        "MSRP": order_line["MSRP"],
        "Weight": order_line["Weight"],
        "IsDropShip": order_line["IsDropShip"],
        "RetailPrice": order_line["RetailPrice"],
        "ProductName": order_line["Description"],
        "ProductCategory": find_in_extended_attribute("Category"),
        "Engraved": find_in_extended_attribute("Engraved"),
        "Vendor": find_in_extended_attribute("Vendor"),
        "VariantTitle": find_in_extended_attribute("VariantTitle"),
        "ExtendedCategory": find_in_extended_attribute("Category"),
        "AlcoholCategory": find_in_extended_attribute("AlcoholCategory"),
        "IsAlcohol": find_in_extended_attribute("IsAlcohol"),
        "HandmadeLeatherBottleHolder": find_in_extended_attribute("HandmadeLeatherBottleHolder"),
        "LuxuryDrawstring": find_in_extended_attribute("LuxuryDrawstring"),
        "PartnerLineID": find_in_extended_attribute("PartnerLineID"),
        "ExtendedDescription": find_in_extended_attribute("OrderDescription"),
        "LineNumber": order_line["LineNumber"]
    }


def parse_single_order_line_v2(order_line):
    """
    Converts a single order line to a dict containing the relevant fields.
    This implementation is more efficient when extended attributes is not large
    and the number of queried attributes is a relatively high percentage.
    """
    extended_attributes = {attribute["Name"]: attribute["Value"] for attribute in order_line["ExtendedAttributes"]}
    return {
        "Qty": order_line["Quantity"],
        "SKU": order_line["ItemIdentifier"]["SupplierSKU"],
        "UPC": order_line["ItemIdentifier"]["UPC"],
        "Price": order_line["Price"],
        "Cost": order_line["Cost"],
        "MSRP": order_line["MSRP"],
        "Weight": order_line["Weight"],
        "IsDropShip": order_line["IsDropShip"],
        "RetailPrice": order_line["RetailPrice"],
        "ProductName": order_line["Description"],
        "ProductCategory": extended_attributes["Category"],
        "Engraved": extended_attributes["Engraved"],
        "Vendor": extended_attributes["Vendor"],
        "VariantTitle": extended_attributes["VariantTitle"],
        "ExtendedCategory": extended_attributes["Category"],
        "AlcoholCategory": extended_attributes["AlcoholCategory"],
        "IsAlcohol": extended_attributes["IsAlcohol"],
        "HandmadeLeatherBottleHolder": extended_attributes["HandmadeLeatherBottleHolder"],
        "LuxuryDrawstring": extended_attributes["LuxuryDrawstring"],
        "PartnerLineID": extended_attributes["PartnerLineID"],
        "ExtendedDescription": extended_attributes["OrderDescription"],
        "LineNumber": order_line["LineNumber"]
    }


def parse_order(order):
    """
    Converts an order into a dict containing the relevant fields, duplicating
    information that is the same for all order lines.
    """
    order_info = {
        "Order Number": order["OrderNumber"],
        "Order Date": order["OrderDate"],
        "DocumentDate": order["DocumentDate"],
        "Ship Method": order["ShipmentInfos"][0]["ServiceLevelDescription"],
        "Ship To State": order["ShipToAddress"]["State"],
        "CustomerNumber": order["CustomerNumber"],
        "StatusCode": order["StatusCode"],
        "PartnerPO": order["PartnerPO"],
        "PayInNumberOfDays": order["PaymentTerm"]["PayInNumberOfDays"],
        "DiscountInNumberOfDays": order["PaymentTerm"]["DiscountInNumberOfDays"],
        "AvailableDiscount": order["PaymentTerm"]["AvailableDiscount"],
        "TotalAmount": order["TotalAmount"]
    }
    order_lines = [parse_single_order_line_v1(order_line) for order_line in order["OrderLines"]]
    return [dict(order_info, **order_line) for order_line in order_lines]


def parse_all_orders(records):
    """
    Parses every record and returns an array of dicts.
    """
    return [parse_order(record) for record in records]



page = 0
rows = []

while page < Totalpages:
    payload1 = {
        "subscription-key": API_Subscriptionkey,
        "Filters.senderCompanyId": API_SenderCompanyID,
        "Filters.from": API_Filterfrom,
        "Filters.to": API_FilterTo,
        "Filters.status": API_FilterStatus,
        'page': page
    }

    response = requests.get(url=url, params=payload1)
    orders = response.json()
    results = parse_all_orders(orders["Records"])

    for order in results:
        for entireRecord in order:
            order_number = entireRecord['Order Number']
            line_number = entireRecord['LineNumber']
            order_date = entireRecord['Order Date']
            document_date = entireRecord['DocumentDate']
            line_number = entireRecord['LineNumber']
            vendor = entireRecord['Vendor']
            variant_title = entireRecord['VariantTitle']
            extended_category = entireRecord['ExtendedCategory']
            alcohol_category = entireRecord['AlcoholCategory']
            is_alcohol = entireRecord['IsAlcohol']
            hand_made_leather_bottle_holder = entireRecord['HandmadeLeatherBottleHolder']
            luxury_draw_string = entireRecord['LuxuryDrawstring']
            partner_line_id = entireRecord['PartnerLineID']
            customer_number = entireRecord['CustomerNumber']
            status_code = entireRecord['StatusCode']
            total_amount = entireRecord['TotalAmount']
            partner_po = entireRecord['PartnerPO']
            price = entireRecord['Price']
            upc = entireRecord['UPC']
            sku = entireRecord['SKU']
            cost = entireRecord['Cost']
            msrp = entireRecord['MSRP']
            weight = entireRecord['Weight']
            quantity = entireRecord['Qty']
            is_drop_ship = entireRecord['IsDropShip']
            extended_description = entireRecord['ExtendedDescription']
            pay_in_number_of_days = entireRecord['PayInNumberOfDays']
            discount_in_number_of_days = entireRecord['DiscountInNumberOfDays']
            available_discount = entireRecord['AvailableDiscount']
            engraved = entireRecord['Engraved']

        rows.append(
            {
                "order_number": order_number,
                "order_date": order_date,
                "document_date": document_date,
                "line_number": line_number,
                "vendor": vendor,
                "variant_title": variant_title,
                "engraved": engraved,
                "extended_category": extended_category,
                "alcohol_category": alcohol_category,
                "is_alcohol": is_alcohol,
                "luxury_draw_string": luxury_draw_string,
                "partner_line_id": partner_line_id,
                "customer_number": customer_number,
                "status_code": status_code,
                "partner_po": partner_po,
                "total_amount": total_amount,
                "price": price,
                "upc": "upc",
                "sku": sku,
                "cost": cost,
                "msrp": msrp,
                "weight": weight,
                "quantity": quantity,
                "is_drop_ship": is_drop_ship,
                "extended_description": extended_description,
                "pay_in_number_of_days": pay_in_number_of_days,
                "available_discount": available_discount,
                "hand_made_leather_bottle_holder": hand_made_leather_bottle_holder
            })

    page += 1

df = pd.DataFrame(rows,columns=dfcols)
df.to_sql('stg_order2', connStringRed, index=False, if_exists='append', schema='rb')

conn.commit()
conn.close()

print("**Upload to Redshift Completed Successfully ")

