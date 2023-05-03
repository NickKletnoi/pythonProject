import json
import pandas as pd
# pd.set_option("max_columns", None)
# pd.set_option("max_rows", None)
#
# data = [{'state': 'Florida',
#          'shortname': 'FL',
#          'info': {'governor': 'Rick Scott'},
#          'counties': [{'name': 'Dade', 'population': 12345},
#                       {'name': 'Broward', 'population': 40000},
#                       {'name': 'Palm Beach', 'population': 60000}]},
#         {'state': 'Ohio',
#          'shortname': 'OH',
#          'info': {'governor': 'John Kasich'},
#          'counties': [{'name': 'Summit', 'population': 1234},
#                       {'name': 'Cuyahoga', 'population': 1337}]}]
# result = pd.json_normalize(data, 'counties', ['state', 'shortname',['info', 'governor']])
# print(result)


with open('sample2.json','r') as f:
    data = f.readlines()
d = pd.concat([pd.json_normalize(json.loads(j)) for j in data])
d.display()

