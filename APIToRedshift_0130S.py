import requests
import datetime
import pandas as pd
from sqlalchemy import create_engine
import sys
#import time
#import psycopg2
#import json

# define list of columns - same as DB such that no mapping is required later
dfcols = [
        "thread_id",
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
        "order_note",
        "shipfromaddress_contacttype",
        "remittoaddress_contacttype",
        "markforaddress_contacttype",
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
        "shiptoaddress_company",
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
        "billtoaddress_company",
        "billtoaddress_firstname",
        "billtoaddress_lastname",
        "billtoaddress_address1",
        "billtoaddress_address2",
        "billtoaddress_city",
        "billtoaddress_state",
        "billtoaddress_country",
        "billtoaddress_zip",
        "billtoaddress_phone",
        "billtoaddress_contacttype",
        "billtoaddress_email",
        "billtoaddress_taxnumber",
        "billtoaddress_note",
        "line_number",
        "supplier_sku",
        "partner_sku",
        "upc",
        "price",
        "retail_price",
        "cost",
        "msrp",
        "description",
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
        "is_engraved"
]

def parse_single_order_line_v1(order_line, threadid):
    """
    Converts a single order line to a dict containing the relevant fields.
    This implementation is more efficient when extended attributes is large
    and the number of queried attributes is a relatively low percentage.
    """

    def find_in_extended_attribute(attribute_name):
        s1 = ''
        for entry in order_line["ExtendedAttributes"]:
            if entry["Name"] == attribute_name:
                if "Value" in entry:
                    s1 = entry["Value"]
                break;
        return s1;

    return {
        "thread_id": threadid,
        "line_number": order_line["LineNumber"],
        "supplier_sku": order_line["ItemIdentifier"]["SupplierSKU"] if "SupplierSKU" in order_line["ItemIdentifier"] else "",
        "partner_sku": order_line["ItemIdentifier"]["PartnerSKU"] if "PartnerSKU" in order_line["ItemIdentifier"] else "",
        "upc": order_line["ItemIdentifier"]["UPC"] if "UPC" in order_line["ItemIdentifier"] else "",
        "price": order_line["Price"] if "Price" in order_line else 0.00,
        "cost": order_line["Cost"] if "Cost" in order_line else 0.00,
        "msrp": order_line["MSRP"] if "MSRP" in order_line else 0.00,
        "description": order_line["Description"][:999] if "Description" in order_line else "",
        "discount_amount": order_line["Discounts"][0]["DiscountAmount"] if len(order_line["Discounts"])>0 and "DiscountAmount" in order_line["Discounts"][0] else 0,
        "is_dropship": order_line["IsDropShip"] if "IsDropShip" in order_line else "",
        "quantity": order_line["Quantity"] if "Quantity" in order_line else 0,
        "quantity_uom": order_line["QuantityUOM"] if "QuantityUOM" in order_line else "",
        "weight": order_line["Weight"] if "Weight" in order_line else 0.00,
        "vendor": find_in_extended_attribute("Vendor"),
        "variant_title": find_in_extended_attribute("VariantTitle"),
        "retail_price": order_line["RetailPrice"] if "RetailPrice" in order_line else "",
        "category": find_in_extended_attribute("Category"),
        "alcohol_category": find_in_extended_attribute("AlcoholCategory"),
        "is_alcohol": find_in_extended_attribute("IsAlcohol"),
        "has_bottleholder": find_in_extended_attribute("HandmadeLeatherBottleHolder"),
        "has_drawstring": find_in_extended_attribute("LuxuryDrawstring"),
        "is_engraved": find_in_extended_attribute("Engraved")
    }

def parse_order(order, threadid):
    """
    Converts an order into a dict containing the relevant fields, duplicating
    information that is the same for all order lines.
    """
    extended_attributes = {attribute["Name"]: attribute["Value"] for attribute in order["ExtendedAttributes"] if ("Value" in order["ExtendedAttributes"] and "Name" in order["ExtendedAttributes"])}

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
        "order_note": order["Note"][:999],
        "shipfromaddress_contacttype": order["ShipFromAddress"]["ContactType"] if "ContactType" in order["ShipFromAddress"] else "",
        "remittoaddress_contacttype": order["RemitToAddress"]["ContactType"] if "ContactType" in order["RemitToAddress"] else "",
        "markforaddress_contacttype": order["MarkForAddress"]["ContactType"] if "ContactType" in order["MarkForAddress"] else "",
        "tax_amount": order["Taxes"][0]["TaxAmount"] if len(order["Taxes"])>0 and "TaxAmount" in order["Taxes"][0] else 0,
        "tax_rate": order["Taxes"][0]["TaxRate"] if len(order["Taxes"])>0 and "TaxRate" in order["Taxes"][0] else 0,
        "pay_method": order["Payments"][0]["Method"] if len(order["Payments"])>0 and "Method" in order["Payments"][0] else "",
        "pay_amount": order["Payments"][0]["Amount"] if len(order["Payments"])>0 and "Amount" in order["Payments"][0] else 0,
        "pay_reference_id": order["Payments"][0]["ReferenceId"] if len(order["Payments"])>0 and "ReferenceId" in order["Payments"][0] else "",
        "payterm_payin_numdays": order["PaymentTerm"]["PayInNumberOfDays"] if "PayInNumberOfDays" in order["PaymentTerm"] else 0,
        "payterm_discount_in_numdays": order["PaymentTerm"]["DiscountInNumberOfDays"] if "DiscountInNumberOfDays" in order["PaymentTerm"] else 0,
        "payterm_available_discount": order["PaymentTerm"]["AvailableDiscount"] if "AvailableDiscount" in order["PaymentTerm"] else 0,
        "payterm_discountpercent": order["PaymentTerm"]["DiscountPercent"] if "DiscountPercent" in order["PaymentTerm"] else 0,
        "payterm_duedate": order["PaymentTerm"]["DueDate"] if "DueDate" in order["PaymentTerm"] else "",
        "payterm_discount_duedate": order["PaymentTerm"]["DiscountDueDate"] if "DiscountDueDate" in order["PaymentTerm"] else "",
        "payterm_effective_date": order["PaymentTerm"]["EffectiveDate"] if "EffectiveDate" in order["PaymentTerm"] else "",
        "shipment_carriercode": order["ShipmentInfos"][0]["CarrierCode"] if len(order["ShipmentInfos"])>0 and "CarrierCode" in order["ShipmentInfos"][0] else "",
        "shipment_classcode": order["ShipmentInfos"][0]["ClassCode"] if len(order["ShipmentInfos"])>0 and "ClassCode" in order["ShipmentInfos"][0] else "",
        "shipment_servicelevel": order["ShipmentInfos"][0]["ServiceLevelDescription"] if len(order["ShipmentInfos"])>0 and "ServiceLevelDescription" in order["ShipmentInfos"][0] else "",
        "shipment_cost": order["ShipmentInfos"][0]["ShipmentCost"] if len(order["ShipmentInfos"])>0 and "ShipmentCost" in order["ShipmentInfos"][0] else 0,
        "internal_order_number": extended_attributes["InternalOrderNumber"] if "InternalOrderNumber" in extended_attributes else "",
        "has_alcohol": extended_attributes["HasAlcohol"] if "HasAlcohol" in extended_attributes else "",
        "sender_shipmethod": extended_attributes["SenderShipMethod"] if "SenderShipMethod" in extended_attributes else "",
        "discount_code": extended_attributes["DiscountCode"] if "DiscountCode" in extended_attributes else "",
        "signature_required": extended_attributes["SignatureRequired"] if "SignatureRequired" in extended_attributes else "",
        "requested_service_level": extended_attributes["RequestedServiceLevel"] if "RequestedServiceLevel" in extended_attributes else "",
        "shiptoaddress_company": order["ShipToAddress"]["CompanyName"][:99] if "CompanyName" in order["ShipToAddress"] else "",
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
        "billtoaddress_company": order["BillToAddress"]["CompanyName"][:99] if "CompanyName" in order["BillToAddress"] else "",
        "billtoaddress_firstname": order["BillToAddress"]["FirstName"] if "FirstName" in order["BillToAddress"] else "",
        "billtoaddress_lastname": order["BillToAddress"]["LastName"] if "LastName" in order["BillToAddress"] else "",
        "billtoaddress_address1": order["BillToAddress"]["Address1"] if "Address1" in order["BillToAddress"] else "",
        "billtoaddress_address2": order["BillToAddress"]["Address2"] if "Address2" in order["BillToAddress"] else "",
        "billtoaddress_city": order["BillToAddress"]["City"] if "City" in order["BillToAddress"] else "",
        "billtoaddress_state": order["BillToAddress"]["State"] if "State" in order["BillToAddress"] else "",
        "billtoaddress_country": order["BillToAddress"]["Country"] if "Country" in order["BillToAddress"] else "",
        "billtoaddress_zip": order["BillToAddress"]["Zip"] if "Zip" in order["BillToAddress"] else "",
        "billtoaddress_phone": order["BillToAddress"]["Phone"] if "Phone" in order["BillToAddress"] else "",
        "billtoaddress_contacttype": order["BillToAddress"]["ContactType"] if "ContactType" in order["BillToAddress"] else "",
        "billtoaddress_email": order["BillToAddress"]["Email"] if "Email" in order["BillToAddress"] else "",
        "billtoaddress_taxnumber": order["BillToAddress"]["TaxNumber"] if "TaxNumber" in order["BillToAddress"] else "",
        "billtoaddress_note": order["BillToAddress"]["Note"][:999] if "Note" in order["BillToAddress"] else ""
    }
    order_lines = [parse_single_order_line_v1(order_line, threadid) for order_line in order["OrderLines"]]
    return [dict(order_info, **order_line) for order_line in order_lines]

def parse_all_orders(records, threadid):
    """
    Parses every record and returns an array of dicts.
    """
    return [parse_order(record, threadid) for record in records]

def db_truncate_stg(engine, schemaname, threadid):
    """
    cleanup staging table for this thread
    """
    qry = "delete " + schemaname + ".stg_order_item21 where thread_id = " + str(threadid)
    conn = engine.raw_connection()
    cursor = conn.cursor()
    cursor.execute(qry)
    cursor.close()
    conn.commit()
    
def db_move_to_final(engine, schemaname, threadid, api_start, numrec, tm_api, tm_parse, tm_db, totalrec, api_end, starttime):
    """
    move data to final tables by calling proc, then write timing stats, then truncate staging
    """
    print("**** flushing out to main tables")
    time1 = datetime.datetime.now()
    qry = "CALL " + schemaname + ".sp_insert_order_v2("+str(threadid)+");"
    conn = engine.raw_connection()
    cursor = conn.cursor()
    cursor.execute(qry)
    x = datetime.datetime.now() - time1
    tdiff = x.total_seconds()

    qry = "CALL " + schemaname + ".sp_insert_jobdetail_stat(" + str(threadid) + ",'" + api_start + "'," + str(numrec) +"," + str(tm_api) + "," + str(tm_parse) + "," + str(tm_db) + ")"
    cursor.execute(qry)
    if totalrec >= 0:
        qry = "CALL " + schemaname + ".sp_insert_job_stat(" + str(threadid) + ",'" + api_start + "','" + api_end + "'," + str(totalrec) +",'" + str(starttime) + "'," + str(tdiff) +")"
        cursor.execute(qry)
    
    cursor.close()
    conn.commit()
    
    
def process_timeslot( threadid, baseurl, subkey, companyid, engine, schemaname, api_start, api_end):
    """
    call API using url and time range, and process records into DB using dbcon
    threadid allows parallel execution ... using thread_id column in staging table
    """
    # init vars
    pagesize = 50 #max pagesize for LogicBroker
    bufpage  = 20 #flush out to final table after every 20 pages, or 1000 records
    page = 0
    numorders = -1
    tdiff1 = tdiff2 = tdiff3 = 0.0  # time diff in seconds between API, json processing & DB
    starttime = datetime.datetime.now()
    db_truncate_stg(engine, schemaname, threadid) 
    
    # call API, and loop as long as there are pagesize # of records coming back
    while True:
        time1 = datetime.datetime.now()
        payload = {
            "subscription-key": subkey,
            "Filters.senderCompanyId": companyid,
            "Filters.from": api_start,
            "Filters.to": api_end,
            "Filters.pageSize": pagesize,
            "Filters.page": page
        }
        response = requests.get(url=baseurl, params=payload)
        #with open('c:/ness/reservebar/v2large.json', 'r') as json_file:
        #    orders = json.load(json_file)
        orders = response.json()
        if not( "Records" in orders ):
            print( 'API Failed' )
            print( orders )
            sys.exit()
        if numorders<0: 
            numorders = orders["TotalRecords"]
            print("Number of records from LogicBroker =", numorders)
        time2 = datetime.datetime.now()
        x = time2 - time1
        tdiff1 += x.total_seconds()
        
        # Parse the jsons and add all orderlines for 50 orders to rows[]
        results = parse_all_orders(orders["Records"], threadid)
        rows = []
        for order in results:
            for orderline in order:
                rows.append(orderline)
        
        time3 = datetime.datetime.now()
        x = time3 - time2
        tdiff2 += x.total_seconds()

        df = pd.DataFrame(rows,columns=dfcols)
        df.to_sql("stg_order_item21", engine, index=False, if_exists='append', schema=schemaname, method='multi')
            
        x = datetime.datetime.now() - time3
        tdiff3 += x.total_seconds()
        if len(orders["Records"]) < pagesize :   #got fewer records than pagesize - so beak loop
            break;
        page += 1
        if page%bufpage==0: 
            #flush out into final tables every 1000 orders
            #db_move_to_final(engine, schemaname, threadid, api_start, pagesize*bufpage, tdiff1, tdiff2, tdiff3, -1, "", "" )
            tdiff1 = tdiff2 = tdiff3 = 0.0 

    # move any remaining data to final and write job stats
    n = len(orders["Records"])
    #db_move_to_final(engine, schemaname, threadid, api_start, n, tdiff1, tdiff2, tdiff3, n+(page*pagesize), api_end, starttime )


# starts here
conStr = 'postgresql+psycopg2://reservebar-master:0$sd^e2ivN!9xP!MO4Mr@reservebar-master.cosesp8bmzst.us-west-2.redshift.amazonaws.com:5439/reservebar-master'
engine = create_engine(conStr);
baseurl = 'https://commerceapi.io/api/v2/Orders'
subkey = '528D61CA-E652-465F-8310-1645490A0857'
companyid = '127409'  # corresponds to the company ReserveBar for LogicBroker
schema = 'rb'
api_start = '2020-11-01'
api_end = '2020-11-03'
threadid = 8

process_timeslot( threadid, baseurl, subkey, companyid, engine, schema, api_start, api_end)


print("**Upload to Redshift Completed Successfully ")