import Include
import json
import json_flatten
import extract
import requests
import pandas as pd
from sqlalchemy import create_engine
connStringRed = create_engine(Include.setConnRed())

f = open('V2Order.json')
data = json.load(f)

Totalpages = data['TotalPages']
TotalRecords = data['TotalRecords']
#ord_prn=json.dumps(data, indent=4)
#print(ord_prn)
print('there are total of %s pages in this document' % (Totalpages))
#print(TotalRecords)
#
def flatten_json(nested_json):

    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(nested_json)
    return out


def json_extract(obj, key):
    """Recursively fetch values from nested JSON."""
    arr = []

    def extract(obj, arr, key):
        """Recursively search for values of key in JSON tree."""
        if isinstance(obj, dict):
            for k, v in obj.items():
                if isinstance(v, (dict, list)):
                    extract(v, arr, key)
                elif k == key:
                    arr.append(v)
        elif isinstance(obj, list):
            for item in obj:
                extract(item, arr, key)
        return arr

    values = extract(obj, arr, key)
    return values


#
# dfcols = [
#     "order_number",
#     "order_date"
#     ]
# rows = []
#
# names = json_extract(data, 'Name')
# values = json_extract(data, 'Value')
# print(names)
# print(values)
#
# schema_dict = dict(zip(names, values))
# schema_dict_prn =json.dumps(schema_dict, indent=4)
# print(schema_dict_prn)


# # durations = values[1::2]  # Get every even-index value from a list
# only_engraved_names = [x for x in names if x=='Engraved']
# #
# print(only_engraved_names)

# >>> A=[(1,'A'),(2,'H'),(3,'K'),(4,'J')]
# >>> [y for x,y in A if x>2]
# ['K', 'J']

# >>> A=[(1,'A'),(2,'H'),(3,'K'),(4,'J')]
# >>> [x for x,x in values if x=='Engraved']
# ['K', 'J']

# column_names = ['index', 'first_name', 'last_name', 'join_date']
# column_datatypes = ['integer', 'string', 'string', 'date']

# schema_dict = dict(zip(column_names, column_datatypes))
# print(schema_dict)

for i, order in enumerate(data['Records']):
    engraved = order['ExtendedAttributes'][1]['Value']
    order = order['OrderNumber']
    # if (engraved_name=='Engraved'):
    #         engraved_value=order['OrderLines'][0]['ExtendedAttributes'][10]['Value']
    # else:
    #         engraved_value='-'
    # print('Category Name is: <<< ' + engraved_name + ' >>>  Category Value is: <<<' + engraved_value + '>>>')
    # print(engraved_value)
    print(order)
    print(engraved)

result = json_flatten.flatten(data)
#print(result)
df = pd.Series(result).to_frame()
df.to_csv('JSON_POINTERS2.csv')

# Define a function to search the item
item = 'Engraved'
def search_value(name):
 for i, keyval in enumerate(data['Records'][1]['ExtendedAttributes']):
  if name.lower() == keyval['Name'].lower():
   return keyval['Value']

# Check the return value and print message
if (search_value(item) != None):
  print("The Engraved  is:", search_value(item))
else:
  print("Not Engraved")

#################################################################################

file = 'V2Order.json'
name = 'VariantTitle'
val = 'myvalue'
f = open(file)
data = json.load(f)

def search_and_replace_value(name, value):
        for i, keyval in enumerate(data['Records']['ExtendedAttributes']):
            if name.lower() == keyval['Name'].lower():
                keyval['Value'] = value
        with open(file, 'w') as f:
            json.dump(data, f)

search_and_replace_value(name,val)



    #order_lines_prn = json.dumps(line_number, indent=4)
    #order_date = order['OrderLines']
    #print('the name is: %s and value is: %s' % (line_number_name,line_number_value))
    #print(order_lines)
    # rows.append(
    #         {
    #             "order_number": order_number,
    #             "order_date": order_date
    #         })


# rows_prn=json.dumps(rows, indent=4)
# #print(rows_prn)

f.close()