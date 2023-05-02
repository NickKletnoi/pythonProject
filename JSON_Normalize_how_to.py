import json
import pandas as pd
from json_flatten import flatten

from collections import defaultdict
import time
import numpy as np

a_dict = {
    'school': 'ABC primary school',
    'location': 'London',
    'ranking': 2,
}
df = pd.json_normalize(a_dict)
#print(df)

json_list = [
    { 'class': 'Year 1', 'student number': 20, 'room': 'Yellow' },
    { 'class': 'Year 2', 'student number': 25, 'room': 'Blue' },
]
df = pd.json_normalize(json_list)
#print(df)

json_list = [
    { 'class': 'Year 1', 'num_of_students': 20, 'room': 'Yellow' },
    { 'class': 'Year 2', 'room': 'Blue' }, # no num_of_students
]
df=pd.json_normalize(json_list)
#print(df)


json_obj = {
    'school': 'ABC primary school',
    'location': 'London',
    'ranking': 2,
    'info': {
        'president': 'John Kasich',
        'contacts': {
          'email': {
              'admission': 'admission@abc.com',
              'general': 'info@abc.com'
          },
          'tel': '123456789',
      }
    }
}

df=pd.json_normalize(json_obj)
#df=pd.json_normalize(json_obj,max_level=1)
#print(df)


json_list = [
    {
        'class': 'Year 1',
        'student count': 20,
        'room': 'Yellow',
        'info': {
            'teachers': {
                'math': 'Rick Scott',
                'physics': 'Elon Mask'
            }
        }
    },
    {
        'class': 'Year 2',
        'student count': 25,
        'room': 'Blue',
        'info': {
            'teachers': {
                'math': 'Alan Turing',
                'physics': 'Albert Einstein'
            }
        }
    },
]
df = pd.json_normalize(json_list)
#print(df)


###################################################

json_obj = [{
    'school': 'ABC primary school',
    'location': 'London',
    'ranking': 2,
    'info': {
        'president': 'John Kasich',
        'contacts': {
          'email': {
              'admission': 'admission@abc.com',
              'general': 'info@abc.com'
          },
          'tel': '123456789',
      }
    },
    'students': [
      { 'name': 'Tom' },
      { 'name': 'James' },
      { 'name': 'Jacqueline' }
    ],
},
{
    'school': 'Oxford School',
    'location': 'Halifax',
    'ranking': 2,
    'info': {
        'president': 'John Knisel',
        'contacts': {
          'email': {
              'admission': 'admission@oxfordschool.com',
              'general': 'info@oxfordschool.com'
          },
          'tel': '123456789',
      }
    },
    'students': [
      { 'name': 'Nick' },
      { 'name': 'Dick' },
      { 'name': 'Jane' },
      {'name': 'Cindy'},
    ],
}]

pd.set_option('display.max_columns', None)

#df= pd.json_normalize(json_obj,record_path=['students'],meta=['school','location','ranking', ['info', 'contacts', 'tel'],['info','president']])
df= pd.json_normalize(json_obj,record_path=['students'],meta=['school','location','ranking', ['info', 'contacts', 'tel'],['info','president']],errors='ignore',meta_prefix='cls_',record_prefix='std_')

#print(df)



########################################
this_dict = {'events': [
  {'id': 142896214,
   'playerId': 37831,
   'teamId': 3157,
   'matchId': 2214569,
   'matchPeriod': '1H',
   'eventSec': 0.8935539999999946,
   'eventId': 8,
   'eventName': 'Pass',
   'subEventId': 85,
   'subEventName': 'Simple pass',
   'positions': [{'x': 51, 'y': 49}, {'x': 40, 'y': 53}],
   'tags': [{'id': 1801, 'tag': {'label': 'accurate'}}]},
 {'id': 142896214,
   'playerId': 37831,
   'teamId': 3157,
   'matchId': 2214569,
   'matchPeriod': '1H',
   'eventSec': 0.8935539999999946,
   'eventId': 8,
   'eventName': 'Pass',
   'subEventId': 85,
   'subEventName': 'Simple pass',
   'positions': [{'x': 51, 'y': 49}, {'x': 40, 'y': 53},{'x': 51, 'y': 49}],
   'tags': [{'id': 1801, 'tag': {'label': 'accurate'}}]}
]}

def flatten_json(nested_json, exclude=['']):
    """Flatten json object with nested keys into a single level.
        Args:
            nested_json: A nested json object.
            exclude: Keys to exclude from output.
        Returns:
            The flattened json object if successful, None otherwise.
    """
    out = {}

    def flatten(x, name='', exclude=exclude):
        if type(x) is dict:
            for a in x:
                if a not in exclude: flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(nested_json)
    return out

pd.set_option('display.max_columns', None)
#df3=pd.DataFrame([flatten_json(x) for x in this_dict['events']])
df4=pd.DataFrame([flatten_json(x,['ranking','location']) for x in json_obj])

df4.rename(columns = {
    'info_contacts_email_general':'contacts_email',
    'info_contacts_tel':'contacts_telephone',
    'students_0_name':'first_students_name',
    'students_1_name':'second_students_name',
    'students_2_name': 'third_students_name',
    'students_3_name': 'fourth_students_name',
                      },
         inplace = True)

#print(df4)

#with open('phil_orch.json') as f: d = json.load(f)

pd.set_option('display.width', 200)
#df8 = pd.json_normalize(data=d['programs'], record_path='concerts',meta=['id', 'orchestra', 'programID', 'season'],errors='ignore',meta_prefix='parent_',record_prefix='child_')
#df9 = pd.json_normalize(data=d['programs'], record_path='works,soloists',meta=['id', 'orchestra', 'programID', 'season'],errors='ignore',meta_prefix='parent_',record_prefix='child_')

#print(df9)

def makeshit():

    with open('V2Order1.json') as o: d = json.load(o)
    rows = []
    for order in d['Records']:
            time.sleep(0.1)
            retailer_id = order['ReceiverCompanyId']
            order_number = order['OrderNumber']
            order_date = order['OrderDate']
            document_date = order['DocumentDate']
            status = order['StatusCode']
            logic_broker_key = order['Identifier']['LogicbrokerKey']
            po_number = order['PartnerPO']
            state = order['ShipToAddress']['State']
            # extended_attributes_ord = {attribute_ord["Name"]: attribute_ord["Value"] for attribute_ord in order["ExtendedAttributes"]}
            # engraved =extended_attributes_ord["Engraved"] if "Engraved" in extended_attributes_ord else ""

            for extendedattr in order['ExtendedAttributes']:
                if 'Engraved' in extendedattr['Name']: engraved = extendedattr['Value']

            for orderline in order['OrderLines']:
                    upc = orderline['ItemIdentifier']['UPC']
                    price = orderline['Price']
                    product_name = orderline['Description']
                    quantity = orderline['Quantity']
                    line_number = orderline['LineNumber']
                    supplier_sku = orderline['ItemIdentifier']['SupplierSKU']

                    for extendedattr_line in orderline['ExtendedAttributes']:
                        if 'HandmadeLeatherBottleHolder' in extendedattr_line['Name']: HandmadeLeatherBottleHolder = extendedattr_line['Value']

                    for extendedattr_line in orderline['ExtendedAttributes']:
                        if 'Vendor' in extendedattr_line['Name']: vendor = extendedattr_line['Value']

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
                            "state": state,
                            "engraved": engraved,
                            "HandmadeLeatherBottleHolder": HandmadeLeatherBottleHolder
                        })
    df27 = pd.DataFrame(rows)
    print(df27)

p = makeshit()


