import json
import pandas as pd
from collections import defaultdict
import time
import numpy as np
# from pandas.io.json import pandas.json_normalize
#
# ## uppack json - 1st approach
# df = pd.read_json("employee_data.json")
# bn=pd.DataFrame(df.features.values.tolist())['candidate']
# df2= pd.DataFrame.from_records(bn).head(10)
# print(df2)
#
# ## unpack json - 2nd approach
# df = pd.read_json("employee_data.json")
# bn=pd.DataFrame(df.features.values.tolist())['candidate']
# df3 = pd.json_normalize(bn).head(10)
# print(df3)

# ## unpack json - 3rd approach
# f=[]
# data = pd.read_json("employee_data.json")
# for i in data['features']:
#     f.append(i['candidate'])
# df4 = pd.DataFrame(f).head(10)
# print(df4)

## unpack json - 4th approach
data5 = pd.read_json("employee_data.json")
tt=[val for sublist in data5['features'] for val in sublist.values()]
df5 = pd.DataFrame(tt).head(10)
#print("employee_data is: ", df5)

#
# with open('phil_orch.json') as f:
#     d = json.load(f)
#     works_data = json_normalize(data=d['programs'], record_path='works',
#                             meta=['id', 'orchestra','programID', 'season'])
#

param = pd.read_json("param_config.json")
lifecycle = param['workspaceEnvironmentDict']['lifecycle']
edwvault_firstscope = param['workspaceEnvironmentDict']['edwDataLakeKeyVaultScope']['firstscope']
edwvault_secondscope = param['workspaceEnvironmentDict']['edwDataLakeKeyVaultScope']['secondscope']


print(lifecycle)
print(edwvault_firstscope)
print(edwvault_secondscope)



