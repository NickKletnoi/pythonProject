# -*- coding: utf-8 -*-
"""
Created on Sun Jan 31 19:29:32 2021

@author: P5104579
"""
import requests
import datetime
import pandas as pd
from sqlalchemy import create_engine
import sys


# define list of columns in order - same # and sequence as DB such that no additional mappings are required
dfOrder = [
        "order_number",
        "status_code",
        "sender_company_id",
        "receiver_company_id",
        "logicbroker_key",
        "source_key",
        "retailer_name",
        "link_key",
        "document_date",
        "partner_po",
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
        "credit_card_fee",
        "remittance_to_retailer",
        "shipping_remittance",
        "corrugated_remittance",
        "gross_retailer_remittance",
        "create_time",
	    "last_updated",
	    "updated_by",
    	"estimated_shipdate",
        "total_price",
        "total_quantity",
        "date_shipped",
        "date_shipped_asn"
]

# define list of columns in order-line (or item) - same # & sequence as DB
dfItem = [
        "order_number",
        "partner_po",
        "line_number",
        "supplier_sku",
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
        "is_engraved",
        "item_price",
        "create_time",
	    "last_updated",
	    "updated_by",
    	"partner_sku",
        "document_date"
]

def find_in_extended_attribute(rec, attribute_name, deflt):
    s1 = deflt    # default value if the field is not found in extended attributes
    for entry in rec["ExtendedAttributes"]:
        if entry["Name"] == attribute_name:
            if "Value" in entry:
                s1 = entry["Value"]
            break;
    return s1;

def parse_single_order_line(order, order_line, updtime, upduser):
    """
    Converts a single order line to a dict containing the relevant fields.
    This implementation is more efficient when extended attributes is large
    and the number of queried attributes is a relatively low percentage.
    """
    return {
        "document_date": order["document_date"],
        "order_number": order["order_number"],
        "partner_po": order["partner_po"],
        "line_number": order_line["LineNumber"],
        "supplier_sku": order_line["ItemIdentifier"]["SupplierSKU"] if "SupplierSKU" in order_line["ItemIdentifier"] else "",
        "partner_sku": order_line["ItemIdentifier"]["PartnerSKU"] if "PartnerSKU" in order_line["ItemIdentifier"] else "",
        "upc": order_line["ItemIdentifier"]["UPC"] if "UPC" in order_line["ItemIdentifier"] else "",
        "price": order_line["Price"] if "Price" in order_line else 0.00,
        "cost": order_line["Cost"] if "Cost" in order_line else 0.00,
        "msrp": order_line["MSRP"] if "MSRP" in order_line else 0.00,
        "description": order_line["Description"][:900] if "Description" in order_line else "",
        "discount_amount": order_line["Discounts"][0]["DiscountAmount"] if len(order_line["Discounts"])>0 and "DiscountAmount" in order_line["Discounts"][0] else 0,
        "is_dropship": order_line["IsDropShip"] if "IsDropShip" in order_line else "",
        "quantity": order_line["Quantity"] if "Quantity" in order_line else 0,
        "quantity_uom": order_line["QuantityUOM"] if "QuantityUOM" in order_line else "",
        "weight": order_line["Weight"] if "Weight" in order_line else 0.00,
        "retail_price": order_line["RetailPrice"] if "RetailPrice" in order_line else "",

        "vendor": find_in_extended_attribute(order_line, "Vendor",''),
        "variant_title": find_in_extended_attribute(order_line, "VariantTitle",''),
        "category": find_in_extended_attribute(order_line, "Category",''),
        "alcohol_category": find_in_extended_attribute(order_line, "AlcoholCategory",''),
        "is_alcohol": find_in_extended_attribute(order_line, "IsAlcohol",''),
        "has_bottleholder": find_in_extended_attribute(order_line, "HandmadeLeatherBottleHolder",''),
        "has_drawstring": find_in_extended_attribute(order_line, "LuxuryDrawstring",''),
        "is_engraved": find_in_extended_attribute(order_line, "Engraved",''),

        "create_time": updtime,
	    "last_updated": updtime,
	    "updated_by": upduser
    }

def parse_single_order(OrderArr, ItemArr, order, updtime, upduser):
    """
    Converts an order into a dict containing the relevant fields, duplicating
    information that is the same for all order lines.
    """
    #print(order["OrderNumber"])

    order_info = {
        "order_number": order["OrderNumber"],
        "partner_po": order["PartnerPO"],
        "status_code": order["StatusCode"],
        "sender_company_id": order["SenderCompanyId"],
        "receiver_company_id": order["ReceiverCompanyId"],
        "logicbroker_key": order["Identifier"]["LogicbrokerKey"],
        "source_key": order["Identifier"]["LogicbrokerKey"],
        "link_key": order["Identifier"]["LinkKey"],
        "document_date": order["DocumentDate"].replace('T',' '),  #  .... include this for csv
        "order_date": order["OrderDate"][:10],
        "total_amount": order["TotalAmount"],
        "currency": order["Currency"][:5],
        "handling_amount": order["HandlingAmount"],
        "dropship_amount": order["DropshipAmount"],
        "order_note": order["Note"][:200],
        "shipfromaddress_contacttype": order["ShipFromAddress"]["ContactType"] if "ContactType" in order["ShipFromAddress"] else "",
        "remittoaddress_contacttype": order["RemitToAddress"]["ContactType"] if "ContactType" in order["RemitToAddress"] else "",
        "markforaddress_contacttype": order["MarkForAddress"]["ContactType"] if "ContactType" in order["MarkForAddress"] else "",
        "tax_amount": order["Taxes"][0]["TaxAmount"] if len(order["Taxes"])>0 and "TaxAmount" in order["Taxes"][0] else 0,
        "tax_rate": order["Taxes"][0]["TaxRate"] if len(order["Taxes"])>0 and "TaxRate" in order["Taxes"][0] else 0,
        "pay_method": order["Payments"][0]["Method"] if "Payments" in order and len(order["Payments"])>0 and "Method" in order["Payments"][0] else "",
        "pay_amount": order["Payments"][0]["Amount"] if "Payments" in order and len(order["Payments"])>0 and "Amount" in order["Payments"][0] else 0,
        "pay_reference_id": order["Payments"][0]["ReferenceId"] if "Payments" in order and len(order["Payments"])>0 and "ReferenceId" in order["Payments"][0] else "",
        "payterm_payin_numdays": order["PaymentTerm"]["PayInNumberOfDays"] if "PayInNumberOfDays" in order["PaymentTerm"] else 0,
        "payterm_discount_in_numdays": order["PaymentTerm"]["DiscountInNumberOfDays"] if "DiscountInNumberOfDays" in order["PaymentTerm"] else 0,
        "payterm_available_discount": order["PaymentTerm"]["AvailableDiscount"] if "AvailableDiscount" in order["PaymentTerm"] else 0,
        "payterm_discountpercent": order["PaymentTerm"]["DiscountPercent"] if "DiscountPercent" in order["PaymentTerm"] else 0,
        "payterm_duedate": order["PaymentTerm"]["DueDate"][:10] if "DueDate" in order["PaymentTerm"] else "0001-01-01",
        "payterm_discount_duedate": order["PaymentTerm"]["DiscountDueDate"][:10] if "DiscountDueDate" in order["PaymentTerm"] else "0001-01-01",
        "payterm_effective_date": order["PaymentTerm"]["EffectiveDate"][:10] if "EffectiveDate" in order["PaymentTerm"] else "0001-01-01",
        "shipment_carriercode": order["ShipmentInfos"][0]["CarrierCode"] if "ShipmentInfos" in order and len(order["ShipmentInfos"])>0 and "CarrierCode" in order["ShipmentInfos"][0] else "",
        "shipment_classcode": order["ShipmentInfos"][0]["ClassCode"] if "ShipmentInfos" in order and len(order["ShipmentInfos"])>0 and "ClassCode" in order["ShipmentInfos"][0] else "",
        "shipment_servicelevel": order["ShipmentInfos"][0]["ServiceLevelDescription"] if "ShipmentInfos" in order and len(order["ShipmentInfos"])>0 and "ServiceLevelDescription" in order["ShipmentInfos"][0] else "",
        "shipment_cost": order["ShipmentInfos"][0]["ShipmentCost"] if "ShipmentInfos" in order and len(order["ShipmentInfos"])>0 and "ShipmentCost" in order["ShipmentInfos"][0] else 0,
        "shiptoaddress_company": order["ShipToAddress"]["CompanyName"][:90] if "CompanyName" in order["ShipToAddress"] else "",
        "shiptoaddress_firstname": order["ShipToAddress"]["FirstName"] if "FirstName" in order["ShipToAddress"] else "",
        "shiptoaddress_lastname": order["ShipToAddress"]["LastName"] if "LastName" in order["ShipToAddress"] else "",
        "shiptoaddress_address1": order["ShipToAddress"]["Address1"][:90] if "Address1" in order["ShipToAddress"] else "",
        "shiptoaddress_address2": order["ShipToAddress"]["Address2"][:90] if "Address2" in order["ShipToAddress"] else "",
        "shiptoaddress_city": order["ShipToAddress"]["City"] if "City" in order["ShipToAddress"] else "",
        "shiptoaddress_state": order["ShipToAddress"]["State"][:4] if "State" in order["ShipToAddress"] else "",
        "shiptoaddress_country": order["ShipToAddress"]["Country"][:8] if "Country" in order["ShipToAddress"] else "",
        "shiptoaddress_zip": order["ShipToAddress"]["Zip"][:10] if "Zip" in order["ShipToAddress"] else "",
        "shiptoaddress_phone": order["ShipToAddress"]["Phone"] if "Phone" in order["ShipToAddress"] else "",
        "shiptoaddress_contacttype": order["ShipToAddress"]["ContactType"] if "ContactType" in order["ShipToAddress"] else "",
        "shiptoaddress_email": order["ShipToAddress"]["Email"][:90] if "Email" in order["ShipToAddress"] else "",
        "billtoaddress_company": order["BillToAddress"]["CompanyName"][:90] if "CompanyName" in order["BillToAddress"] else "",
        "billtoaddress_firstname": order["BillToAddress"]["FirstName"] if "FirstName" in order["BillToAddress"] else "",
        "billtoaddress_lastname": order["BillToAddress"]["LastName"] if "LastName" in order["BillToAddress"] else "",
        "billtoaddress_address1": order["BillToAddress"]["Address1"][:90] if "Address1" in order["BillToAddress"] else "",
        "billtoaddress_address2": order["BillToAddress"]["Address2"][:90] if "Address2" in order["BillToAddress"] else "",
        "billtoaddress_city": order["BillToAddress"]["City"][:90] if "City" in order["BillToAddress"] else "",
        "billtoaddress_state": order["BillToAddress"]["State"][:4] if "State" in order["BillToAddress"] else "",
        "billtoaddress_country": order["BillToAddress"]["Country"][:8] if "Country" in order["BillToAddress"] else "",
        "billtoaddress_zip": order["BillToAddress"]["Zip"][:10] if "Zip" in order["BillToAddress"] else "",
        "billtoaddress_phone": order["BillToAddress"]["Phone"] if "Phone" in order["BillToAddress"] else "",
        "billtoaddress_contacttype": order["BillToAddress"]["ContactType"] if "ContactType" in order["BillToAddress"] else "",
        "billtoaddress_email": order["BillToAddress"]["Email"][:90] if "Email" in order["BillToAddress"] else "",
        "billtoaddress_taxnumber": order["BillToAddress"]["TaxNumber"] if "TaxNumber" in order["BillToAddress"] else "",
        "billtoaddress_note": order["BillToAddress"]["Note"][:500] if "Note" in order["BillToAddress"] else "",

        "retailer_name": find_in_extended_attribute(order, "RBRetailerName",''),
        "internal_order_number": find_in_extended_attribute(order, "InternalOrderNumber",''),
        "has_alcohol": find_in_extended_attribute(order, "HasAlcohol",''),
        "sender_shipmethod": find_in_extended_attribute(order, "SenderShipMethod",''),
        "discount_code": find_in_extended_attribute(order, "DiscountCode",''),
        "signature_required": find_in_extended_attribute(order, "SignatureRequired",''),
        "requested_service_level": find_in_extended_attribute(order, "RequestedServiceLevel",''),
        "estimated_shipdate": find_in_extended_attribute(order, "EstimatedShipDate",'0001-01-01'),
        "date_shipped": find_in_extended_attribute(order, "HeaderDateShipped",'0001-01-01')[:10],
        "date_shipped_asn": find_in_extended_attribute(order, "ASNHeaderDateShipped",'0001-01-01')[:10],

        "create_time": updtime,
	    "last_updated": updtime,
	    "updated_by": upduser
    }

    # process each order_line
    for order_line in order["OrderLines"]:
        ItemArr.append(parse_single_order_line(order_info, order_line, updtime, upduser)) 
    
    total_price = total_quantity = 0    
    for order_line in order["OrderLines"]:
        q = order_line["Quantity"] if "Quantity" in order_line else 0.00
        total_price += order_line["Price"] * q if "Price" in order_line else 0.00
        total_quantity += q
 
    # fill or initialize computed fields
    order_info["total_price"] = total_price
    order_info["total_quantity"] = total_quantity
    order_info["credit_card_fee"] = 0
    order_info["remittance_to_retailer"] = 0
    order_info["shipping_remittance"] = 0
    order_info["corrugated_remittance"] = 0
    order_info["gross_retailer_remittance"] = 0
    OrderArr.append(order_info)
   

def parse_all_orders(OrderArr, ItemArr, orders, updtime, upduser):
    """
    Parses every record and appends to the array of dicts.
    """
    for order in orders:
        parse_single_order(OrderArr, ItemArr, order, updtime, upduser) 
        
   
def file_put( basefname, filenum, OrderArr, ItemArr ):
    df = pd.DataFrame(OrderArr,columns=dfOrder)
    df.to_csv(basefname + 'Orders_' +str(filenum) + '.csv', index=False)
    df = pd.DataFrame(ItemArr,columns=dfItem)
    df.to_csv(basefname + 'Items_' +str(filenum) + '.csv', index=False)
            
    
def process_api_to_file( baseurl, subkey, companyid, basefname, api_start, api_end):
    """
    call API using url and time range, and process records into DB using dbcon
    """
    # init vars
    pagesize = 50 #max pagesize for LogicBroker
    bufpage  = 1000 #flush out to file after so many pages - 50K records in 1 file which is ~50MB
    pagenum = filenum = 0;
    OrderArr = []
    ItemArr = []
    
    numorders = -1
    starttime = datetime.datetime.utcnow()
    upduser = 'PY_LOAD'
    
    # call API, and loop as long as there are pagesize # of records coming back
    while True:
        payload = {
            "subscription-key": subkey,
            "Filters.senderCompanyId": companyid,
            "Filters.from": api_start,
            "Filters.to": api_end,
            #"Filters.partnerPO": '540-1619',
            "Filters.pageSize": pagesize,
            "Filters.page": pagenum
        }
        response = requests.get(url=baseurl, params=payload)
        orders = response.json()
        if not( "Records" in orders ):
            print( 'API Failed' )
            print( orders )
            sys.exit()
        if numorders<0: 
            numorders = orders["TotalRecords"]
            print("Number of records from LogicBroker =", numorders)
        
        # Parse the jsons and add all orderlines for 50 orders and append to OrderArr, ItemArr
        parse_all_orders(OrderArr, ItemArr, orders["Records"], starttime, upduser)
        
        if len(orders["Records"]) < pagesize :   #got fewer records than pagesize - so beak loop
            break;
        pagenum += 1
        if pagenum%50==0: 
            print('Processed '+ str(pagenum*50) + ' records at ', datetime.datetime.now())
        if pagenum%bufpage==0: 
            #flush out into file and init vars every "bufpage" number of pages
            file_put(basefname, filenum, OrderArr, ItemArr)
            filenum += 1
            OrderArr = []
            ItemArr = []
    
    file_put(basefname, filenum, OrderArr, ItemArr)
    print('Start time ', starttime)
    endtime = datetime.datetime.now()
    print('End Time', endtime)
    tdiff = endtime-starttime
    print('Total ', str((filenum*pagesize) + len(OrderArr)), ' Orders processed in ', f'{tdiff.total_seconds()/60:.2f}', ' minutes' );

# get max of document_date from order table; that + 1 msec is what to rread from API
def db_getmaxdate( engine, schemaname, tablename ):
    qry = "select dateadd('millisecond', 1, max(document_date))::varchar(40) as col1 from " + schemaname + "." + tablename
    conn = engine.raw_connection()
    cursor = conn.cursor()
    cursor.execute(qry)
    results = cursor.fetchall()
    s1 = results[0][0].replace(' ','T',1) if len(results)>0 else ''
    cursor.close()
    return s1
    
def db_write( engine, schemaname, OrderArr, ItemArr ):
    """
    puts Order & Item arrays into order & order_item tables, and invokes proc for computed fields
    """
    if len(OrderArr)==0:
        return
    df = pd.DataFrame(OrderArr,columns=dfOrder)
    df.to_sql(name="order", con=engine, index=False, if_exists='append', method='multi', schema=schemaname)
    df = pd.DataFrame(ItemArr,columns=dfItem)
    df.to_sql("order_item", engine, index=False, if_exists='append', method='multi', schema=schemaname)

    conn = engine.raw_connection()
    cursor = conn.cursor()
    numrec = len(OrderArr)
    startdt = OrderArr[0]["document_date"]
    enddt = OrderArr[numrec-1]["document_date"]
    qry = 'CALL rb.sp_calcbydate(\'' + startdt + '\',\'' + enddt + '\');'
    cursor.execute(qry)
    cursor.close()
    conn.commit()
    print( 'Processed ', numrec, ' at ', datetime.datetime.now())
    
# call API using url and time range, and process records into DB using dbcon
def process_api_to_db( engine, schemaname, baseurl, subkey, companyid, api_start, api_end):
    # init vars
    pagesize = 50 #max pagesize for LogicBroker
    bufpage  = 10 #flush out to DB after so many pages
    pagenum = batchnum = 0;
    OrderArr = []
    ItemArr = []
    
    numorders = -1
    starttime = datetime.datetime.utcnow()
    upduser = 'PY_LOAD'

    # call API, and loop as long as there are pagesize # of records coming back
    while True:
        payload = {
            "subscription-key": subkey,
            "Filters.senderCompanyId": companyid,
            "Filters.from": api_start,
            "Filters.to": api_end,
            #"Filters.partnerPO": '900435604',
            "Filters.pageSize": pagesize,
            "Filters.page": pagenum
        }
        response = requests.get(url=baseurl, params=payload)
        orders = response.json()
        if not( "Records" in orders ):
            print( 'API Failed' )
            print( orders )
            sys.exit()
        if numorders<0: 
            numorders = orders["TotalRecords"]
            print('At ', datetime.datetime.now(), ' Number of records from LogicBroker =', numorders)
        
        # Parse the jsons and add all orderlines for 50 orders and append to OrderArr, ItemArr
        parse_all_orders(OrderArr, ItemArr, orders["Records"], starttime, upduser)
        
        if len(orders["Records"]) < pagesize :   #got fewer records than pagesize - so beak loop
            break;
        pagenum += 1
        if pagenum%bufpage==0: 
            #flush out into DB and init vars every "bufpage" number of pages
            db_write(engine, schemaname, OrderArr, ItemArr ) 
            batchnum += 1
            OrderArr = []
            ItemArr = []
    
    db_write(engine, schemaname, OrderArr, ItemArr ) 
    return (batchnum*pagesize*bufpage) + len(OrderArr)
