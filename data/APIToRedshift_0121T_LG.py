import Include
import pyodbc
import requests
import pandas as pd
import sys
import time
import json
from sqlalchemy import create_engine
connStringRed = create_engine(Include.setConnRed())
tic_start_main = time.perf_counter()
from datetime import datetime, timedelta

start_date = "2020-12-18T00:00"
end_date = "2021-01-01T00:00"

filetail = Include.setFileTail()
conStringRed2 = Include.setConnRed2()
url = Include.setAPIURLNewV2()
conn = pyodbc.connect(conStringRed2)
print("**Attaching to Orders Endpoint and Processing Response.**")
API_Subscriptionkey = Include.setAPIsubscriptionkey()
API_SenderCompanyID = Include.setAPIFiltersSenderCompanyID()
API_FilterStatus = Include.setAPIFilterstatus()
filetail_json = Include.setFileTailRbJ()

RBDir = Include.setReserveBarDirLoc()
fileTail = Include.setFileTailRbJ()
final_file = RBDir + 'Order_stg_test_' + fileTail


dfcols = [
        "order_number",
        "partner_po",
        "status_code",
        "sender_company_id",
        "receiver_company_id",
        "logicbroker_key",
        "source_key",
        "retailer_name",
        "link_key",
        "document_date",
        "order_date",
        "total_amount",
        "currency",
        "handling_amount",
        "dropship_amount",
        #"order_note",
        #"shipfromaddress_contacttype",
        #"remittoaddress_contacttype",
        #"markforaddress_contacttype",
        "tax_amount",
        "tax_rate",
        "pay_method",
        "pay_amount",
        "pay_reference_id",
        "payterm_payin_numdays",
        "payterm_discount_in_numdays",
        "payterm_available_discount",
        "payterm_discountpercent",
        "payterm_duedate",
        "payterm_discount_duedate",
        "payterm_effective_date",
        "shipment_carriercode",
        "shipment_classcode",
        "shipment_servicelevel",
        "shipment_cost",
        "internal_order_number",
        "has_alcohol",
        "sender_shipmethod",
        "discount_code",
        "signature_required",
        "requested_service_level",
        #"shiptoaddress_company",
        "shiptoaddress_firstname",
        "shiptoaddress_lastname",
        "shiptoaddress_address1",
        "shiptoaddress_address2",
        "shiptoaddress_city",
        "shiptoaddress_state",
        "shiptoaddress_country",
        "shiptoaddress_zip",
        "shiptoaddress_phone",
        "shiptoaddress_contacttype",
        "shiptoaddress_email",
        #"billtoaddress_company",
        #"billtoaddress_firstname",
        #"billtoaddress_lastname",
        #"billtoaddress_address1",
        #"billtoaddress_address2",
        #"billtoaddress_city",
        #"billtoaddress_state",
        #"billtoaddress_country",
        #"billtoaddress_zip",
        #"billtoaddress_phone",
        #"billtoaddress_contacttype",
        #"billtoaddress_email",
        #"billtoaddress_taxnumber",
        #"billtoaddress_note",
        "line_number",
        "supplier_sku",
        "upc",
        "price",
        "retail_price",
        "cost",
        "msrp",
        #"description",
        "discount_amount",
        "is_dropship",
        "quantity",
        "quantity_uom",
        "weight",
        "vendor",
        "variant_title",
        "category",
        "alcohol_category",
        "is_alcohol",
        "has_bottleholder",
        "has_drawstring",
        "is_engraved",
        "item_price"
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
        "qty": order_line["Quantity"] if "Quantity" in order_line else 0,
        "supplier_sku": order_line["ItemIdentifier"]["SupplierSKU"] if "SupplierSKU" in order_line["ItemIdentifier"] else "",
        "upc": order_line["ItemIdentifier"]["UPC"] if "UPC" in order_line["ItemIdentifier"] else "",
        "price": order_line["Price"] if "Price" in order_line else 0.00,
        "cost": order_line["Cost"] if "Cost" in order_line else 0.00,
        "msrp": order_line["MSRP"] if "MSRP" in order_line else 0.00,
        "weight": order_line["Weight"] if "Weight" in order_line else 0.00,
        "is_dropship": order_line["IsDropShip"] if "IsDropShip" in order_line else "",
        "retail_price": order_line["RetailPrice"] if "RetailPrice" in order_line else "",
        #"ProductName": order_line["Description"] if "Description" in order_line else "",
        #"ProductCategory": find_in_extended_attribute("Category") if "Category" in order_line["ExtendedAttributes"] else "",
        "is_engraved": find_in_extended_attribute("Engraved") if "Engraved" in order_line["ExtendedAttributes"] else "",
        "vendor": find_in_extended_attribute("Vendor") if "Vendor" in order_line["ExtendedAttributes"] else "",
        "variant_title": find_in_extended_attribute("VariantTitle") if "VariantTitle" in order_line["ExtendedAttributes"] else "",
        #"ExtendedCategory": find_in_extended_attribute("Category") if "Category" in order_line["ExtendedAttributes"] else "",
        "alcohol_category": find_in_extended_attribute("AlcoholCategory") if "AlcoholCategory" in order_line["ExtendedAttributes"] else "",
        "is_alcohol": find_in_extended_attribute("IsAlcohol") if "IsAlcohol" in order_line["ExtendedAttributes"] else "",
        "has_bottleholder": find_in_extended_attribute("HandmadeLeatherBottleHolder") if "HandmadeLeatherBottleHolder" in order_line["ExtendedAttributes"] else "",
        "has_drawstring": find_in_extended_attribute("LuxuryDrawstring") if "LuxuryDrawstring" in order_line["ExtendedAttributes"] else "",
        #"partner_lineID": find_in_extended_attribute("PartnerLineID") if "PartnerLineID" in order_line["ExtendedAttributes"] else "",
        "extended_description": find_in_extended_attribute("OrderDescription") if "OrderDescription" in order_line["ExtendedAttributes"] else "",
        "line_number": order_line["LineNumber"],
    }

def parse_single_order_line_v2(order_line):
    """
    Converts a single order line to a dict containing the relevant fields.
    This implementation is more efficient when extended attributes is not large
    and the number of queried attributes is a relatively high percentage.
    """

    extended_attributes = {attribute["Name"]: attribute["Value"] for attribute in order_line["ExtendedAttributes"] if
                           ("Value" in order_line["ExtendedAttributes"] and "Name" in order_line["ExtendedAttributes"])}

    return {
        "line_number": order_line["LineNumber"],
        "supplier_sku": order_line["ItemIdentifier"]["SupplierSKU"] if "SupplierSKU" in order_line["ItemIdentifier"] else "",
        "upc": order_line["ItemIdentifier"]["UPC"] if "UPC" in order_line["ItemIdentifier"] else "",
        "price": order_line["Price"] if "Price" in order_line else 0,
        "retail_price": order_line["RetailPrice"] if "RetailPrice" in order_line else 0,
        "cost": order_line["Cost"] if "Cost" in order_line else 0,
        "msrp": order_line["MSRP"] if "MSRP" in order_line else 0,
        #"description": order_line["Description"] if "Description" in order_line else "",
        "discount_amount": order_line["Discounts"][0]["DiscountAmount"] if "DiscountAmount" in order_line["Discounts"][0] else 0,
        "is_dropship": order_line["IsDropShip"] if "IsDropShip" in order_line else False,
        "quantity": order_line["Quantity"] if "Quantity" in order_line else 0,
        "quantity_uom": order_line["QuantityUOM"] if "QuantityUOM" in order_line else "",
        "weight": order_line["Weight"] if "Weight" in order_line else 0,
        "vendor": extended_attributes["Vendor"] if "Vendor" in extended_attributes else "",
        "variant_title": extended_attributes["VariantTitle"] if "VariantTitle" in extended_attributes else "",
        "category": extended_attributes["Category"] if "Category" in extended_attributes else "",
        "alcohol_category": extended_attributes["AlcoholCategory"] if "AlcoholCategory" in extended_attributes else "",
        "is_alcohol": extended_attributes["IsAlcohol"] if "IsAlcohol" in extended_attributes else "",
        "has_bottleholder": extended_attributes["HandmadeLeatherBottleHolder"] if "HandmadeLeatherBottleHolder" in extended_attributes else "",
        "has_drawstring": extended_attributes["LuxuryDrawstring"] if "LuxuryDrawstring" in extended_attributes else "",
        "is_engraved": extended_attributes["Engraved"] if "Engraved" in extended_attributes else "",
        "item_price": extended_attributes["OrderItemPrice"] if "OrderItemPrice" in extended_attributes else 0

    }

def parse_order(order):
    """
    Converts an order into a dict containing the relevant fields, duplicating
    information that is the same for all order lines.
    """
    extended_attributes = {attribute["Name"]: attribute["Value"] for attribute in order["ExtendedAttributes"]}

    #print(order["OrderNumber"])

    order_info = {
        "order_number": order["OrderNumber"],
        "partner_po": order["PartnerPO"],
        "status_code": order["StatusCode"],
        "sender_company_id": order["SenderCompanyId"],
        "receiver_company_id": order["ReceiverCompanyId"],
        "logicbroker_key": order["Identifier"]["LogicbrokerKey"],
        "source_key": order["Identifier"]["LogicbrokerKey"],
        "retailer_name": extended_attributes["RBRetailerName"] if "RBRetailerName" in extended_attributes else "",
        "link_key": order["Identifier"]["LinkKey"],
        "document_date": order["DocumentDate"],
        "order_date": order["OrderDate"],
        "total_amount": order["TotalAmount"],
        "currency": order["Currency"],
        "handling_amount": order["HandlingAmount"],
        "dropship_amount": order["DropshipAmount"],
        #"order_note": order["Note"],
        #"shipfromaddress_contacttype": order["ShipFromAddress"]["ContactType"] if "ContactType" in order["ShipFromAddress"] else "",
        #"remittoaddress_contacttype": order["RemitToAddress"]["ContactType"] if "ContactType" in order["RemitToAddress"] else "",
        #"markforaddress_contacttype": order["MarkForAddress"]["ContactType"] if "ContactType" in order["MarkForAddress"] else "",
        "tax_amount": order["Taxes"][0]["TaxAmount"] if "TaxAmount" in order["Taxes"][0] else 0,
        "tax_rate": order["Taxes"][0]["TaxRate"] if "TaxRate" in order["Taxes"][0] else 0,
        "pay_method": order["Payments"][0]["Method"] if "Method" in order["Payments"][0] else "",
        "pay_amount": order["Payments"][0]["Amount"] if "Amount" in order["Payments"][0] else 0,
        "pay_reference_id": order["Payments"][0]["ReferenceId"] if "ReferenceId" in order["Payments"][0] else "",
        "payterm_payin_numdays": order["PaymentTerm"]["PayInNumberOfDays"] if "PayInNumberOfDays" in order["PaymentTerm"] else 0,
        "payterm_discount_in_numdays": order["PaymentTerm"]["DiscountInNumberOfDays"] if "DiscountInNumberOfDays" in order["PaymentTerm"] else 0,
        "payterm_available_discount": order["PaymentTerm"]["AvailableDiscount"] if "AvailableDiscount" in order["PaymentTerm"] else 0,
        "payterm_discountpercent": order["PaymentTerm"]["DiscountPercent"] if "DiscountPercent" in order["PaymentTerm"] else 0,
        "payterm_duedate": order["PaymentTerm"]["DueDate"] if "DueDate" in order["PaymentTerm"] else "",
        "payterm_discount_duedate": order["PaymentTerm"]["DiscountDueDate"] if "DiscountDueDate" in order["PaymentTerm"] else "",
        "payterm_effective_date": order["PaymentTerm"]["EffectiveDate"] if "EffectiveDate" in order["PaymentTerm"] else "",
        "shipment_carriercode": order["ShipmentInfos"][0]["CarrierCode"] if "CarrierCode" in order["ShipmentInfos"][0] else "",
        "shipment_classcode": order["ShipmentInfos"][0]["ClassCode"] if "ClassCode" in order["ShipmentInfos"][0] else "",
        "shipment_servicelevel": order["ShipmentInfos"][0]["ServiceLevelDescription"] if "ServiceLevelDescription" in order["ShipmentInfos"][0] else "",
        "shipment_cost": order["ShipmentInfos"][0]["ShipmentCost"] if "ShipmentCost" in order["ShipmentInfos"][0] else 0,
        "internal_order_number": extended_attributes["InternalOrderNumber"] if "InternalOrderNumber" in extended_attributes else "",
        "has_alcohol": extended_attributes["HasAlcohol"] if "HasAlcohol" in extended_attributes else "",
        "sender_shipmethod": extended_attributes["SenderShipMethod"] if "SenderShipMethod" in extended_attributes else "",
        "discount_code": extended_attributes["DiscountCode"] if "DiscountCode" in extended_attributes else "",
        "signature_required": extended_attributes["SignatureRequired"] if "SignatureRequired" in extended_attributes else "",
        "requested_service_level": extended_attributes["RequestedServiceLevel"] if "RequestedServiceLevel" in extended_attributes else "",
        #"shiptoaddress_company": order["ShipToAddress"]["CompanyName"] if "CompanyName" in order["ShipToAddress"] else "",
        "shiptoaddress_firstname": order["ShipToAddress"]["FirstName"] if "FirstName" in order["ShipToAddress"] else "",
        "shiptoaddress_lastname": order["ShipToAddress"]["LastName"] if "LastName" in order["ShipToAddress"] else "",
        "shiptoaddress_address1": order["ShipToAddress"]["Address1"] if "Address1" in order["ShipToAddress"] else "",
        "shiptoaddress_address2": order["ShipToAddress"]["Address2"] if "Address2" in order["ShipToAddress"] else "",
        "shiptoaddress_city": order["ShipToAddress"]["City"] if "City" in order["ShipToAddress"] else "",
        "shiptoaddress_state": order["ShipToAddress"]["State"] if "State" in order["ShipToAddress"] else "",
        "shiptoaddress_country": order["ShipToAddress"]["Country"] if "Country" in order["ShipToAddress"] else "",
        "shiptoaddress_zip": order["ShipToAddress"]["Zip"] if "Zip" in order["ShipToAddress"] else "",
        "shiptoaddress_phone": order["ShipToAddress"]["Phone"] if "Phone" in order["ShipToAddress"] else "",
        "shiptoaddress_contacttype": order["ShipToAddress"]["ContactType"] if "ContactType" in order["ShipToAddress"] else "",
        "shiptoaddress_email": order["ShipToAddress"]["Email"] if "Email" in order["ShipToAddress"] else "",
        #"billtoaddress_company": order["BillToAddress"]["CompanyName"] if "CompanyName" in order["BillToAddress"] else "",
        #"billtoaddress_firstname": order["BillToAddress"]["FirstName"] if "FirstName" in order["BillToAddress"] else "",
        #"billtoaddress_lastname": order["BillToAddress"]["LastName"] if "LastName" in order["BillToAddress"] else "",
        #"billtoaddress_address1": order["BillToAddress"]["Address1"] if "Address1" in order["BillToAddress"] else "",
        #"billtoaddress_address2": order["BillToAddress"]["Address2"] if "Address2" in order["BillToAddress"] else "",
        #"billtoaddress_city": order["BillToAddress"]["City"] if "City" in order["BillToAddress"] else "",
        #"billtoaddress_state": order["BillToAddress"]["State"] if "State" in order["BillToAddress"] else "",
        #"billtoaddress_country": order["BillToAddress"]["Country"] if "Country" in order["BillToAddress"] else "",
        #"billtoaddress_zip": order["BillToAddress"]["Zip"] if "Zip" in order["BillToAddress"] else "",
        #"billtoaddress_phone": order["BillToAddress"]["Phone"] if "Phone" in order["BillToAddress"] else "",
        #"billtoaddress_contacttype": order["BillToAddress"]["ContactType"] if "ContactType" in order["BillToAddress"] else "",
        #"billtoaddress_email": order["BillToAddress"]["Email"] if "Email" in order["BillToAddress"] else "",
        #"billtoaddress_taxnumber": order["BillToAddress"]["TaxNumber"] if "TaxNumber" in order["BillToAddress"] else "",
        #"billtoaddress_note": order["BillToAddress"]["Note"] if "Note" in order["BillToAddress"] else ""
    }
    order_lines = [parse_single_order_line_v1(order_line) for order_line in order["OrderLines"]]
    return [dict(order_info, **order_line) for order_line in order_lines]

def parse_all_orders(records):
    """
    Parses every record and returns an array of dicts.
    """
    return [parse_order(record) for record in records]


start_date_d = datetime.strptime(start_date, '%Y-%m-%dT%H:%M')
end_date_d = datetime.strptime(end_date, '%Y-%m-%dT%H:%M')
delta = end_date_d - start_date_d
n_days = delta.days
n = 1

for n in range(n_days):

    start = start_date_d
    end = start + timedelta(days=1)
    start_date_d = start_date_d + timedelta(days=1)
    start_str = start.strftime('%Y-%m-%dT%H:%M')
    end_str = end.strftime('%Y-%m-%dT%H:%M')

    page = 0


    toolbar_width = 40
    sys.stdout.write("[%s]" % (" " * toolbar_width))
    sys.stdout.flush()
    sys.stdout.write("\b" * (toolbar_width+1))

    payload = {
        "subscription-key": Include.setAPIsubscriptionkey(),
        "Filters.senderCompanyId": Include.setAPIFiltersSenderCompanyID(),
        "Filters.from": start_str,
        "Filters.to": end_str,
        "Filters.pageSize": 50
    }
    response = requests.get(url=url, params=payload)
    # print(response.url)
    print("**Mapping Orders Response to Redshift table and processing pages... please standby...**")
    data = response.json()

    Totalpages = data['TotalPages']
    TotalRecords = data['TotalRecords']

    print("Total pages are %s and total orders are: %s for : %s" % (Totalpages,TotalRecords,start_str))

    while page < Totalpages:
        payload1 = {
            "subscription-key": API_Subscriptionkey,
            "Filters.senderCompanyId": API_SenderCompanyID,
            "Filters.from": start_str,
            "Filters.to": end_str,
            "Filters.pageSize": 50,
            "Filters.page": page,
        }

        response = requests.get(url=url, params=payload1)

        print(response.url)

        sys.stdout.write("#")
        sys.stdout.flush()
        page += 1

        orders = response.json()
        results = parse_all_orders(orders["Records"])

        # json_object = json.dumps(results, indent=4)
        # with open(final_file, "w") as outfile:
        #     outfile.write(json_object)

        rows = []

        for order in results:
            time.sleep(0.1)
            for orderline in order:
                rows.append(orderline)

        df = pd.DataFrame(rows, columns=dfcols)
        df.to_sql('stg_order_item', connStringRed, index=False, if_exists='append', schema='rb')


    sys.stdout.write("]\n")
    tic_end_main = time.perf_counter()

    print("** Moving to final table ... ")

    tic_start_database = time.perf_counter()

    final_load_query = """CALL rb.sp_insert_order();"""
    cursor = conn.cursor()
    cursor.execute(final_load_query)
    cursor.close()
    conn.commit()



    tic_end_database = time.perf_counter()

    total_time = (tic_end_database - tic_start_database) + (tic_end_main - tic_start_main)


    print("**Upload to Redshift for %s Completed Successfully with %s API pages and %s Orders **" % (start_str,Totalpages,TotalRecords))
    print("***********************************************************************************************************")
    print(f"This script Python section runtime was:  {tic_end_main - tic_start_main:0.4f} seconds")
    print(f"This script Redshift section execution runtime was:  {tic_end_database - tic_start_database:0.4f} seconds")
    print(f"This script Total combined runtime (Python + Redshift) was:  {total_time:0.4f} seconds")
    print("***********************************************************************************************************")

conn.close()

