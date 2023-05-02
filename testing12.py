import json
import pandas as pd

with open('V2Order1.json') as o: d = json.load(o)
pd.set_option('display.width', 200)
df12 = pd.json_normalize(data=d['Records'], record_path='OrderLines',meta=['OrderNumber'],errors='ignore')
print(df12)