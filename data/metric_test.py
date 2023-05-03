import Include
import pandas as pd
import json

rows =[]

value = []
start_time = []
end_time = []
inspectedcount = []
function = []
attribute = []
relativeerror =[]
thresholds = []
accounts = []
eventtypes = []
guid = []
routerguid = []
appname = []

dfcols = ['beginTimeSeconds']


with open('payload.json') as json_file:
    data = json.load(json_file)

    five_min_endTimeSeconds = data['totalResult']['total']['endTimeSeconds']
    five_min_inspectedCount = data['totalResult']['total']['inspectedCount']

    for val in data['totalResult']['timeSeries']:
        beginTimeSeconds = val['beginTimeSeconds']
        print(beginTimeSeconds)
        print(data['metadata']['eventType'])
        print(data['metadata']['rawSince'])
        rows.append(
            {"beginTimeSeconds": beginTimeSeconds})

df = pd.DataFrame(rows,columns=dfcols)
#df.to_sql('sales_orders2', connStringRed, index=False, if_exists='replace', schema='rb')
#print(df)
#print(five_min_endTimeSeconds)
#print(five_min_inspectedCount)













    # five_min_precentiles = t['total']['results'][0]['percentiles']
    # five_min_beginTimeSeconds = t['total']['beginTimeSeconds']
    # five_min_endTimeSeconds = t['total']['endTimeSeconds']
    # five_min_inspectedCount = t['total']['inspectedCount']
    #
    # rows.append(
    #         {"percentiles": five_min_precentiles,
    #          "beginTimeSeconds": five_min_beginTimeSeconds,
    #          "endTimeSeconds": five_min_endTimeSeconds,
    #          "inspectedCount": five_min_inspectedCount})
    #
