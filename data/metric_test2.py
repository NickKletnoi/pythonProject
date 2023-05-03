import json

value = []
value2 = []
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

rw = []

with open('payload.json') as json_file:
    data = json.load(json_file)

    InspCount = data['performanceStats']['inspectedCount']
    begTime = data['metadata']['beginTime']
    perTile = data['totalResult']['total']['results'][0]['percentiles']['99']
    begTimeSec = data['totalResult']['total']['beginTimeSeconds']
    appName = data['metadata']['facet']



    RawUntil = data['metadata']['rawUntil']
    wallclock = data['performanceStats']['wallClockTime']
    RawSince = data['metadata']['rawSince']
    function = data['metadata']['contents']['timeSeries']['contents'][0]['attribute']


    for key, val in data.items():
        value2.append(val)

    for key, val in data.items():
        value.append(val)


    n = len(data)

    for index in range(0, n):
        get_time_inspt = list(value[1].values())[1][index]
        get_metri = list(value[4].values())[20]
        get_metri_first = list(get_metri.values())[1]
        get_metri_second = list(get_metri_first.values())[1][0]
        start_time.append(list(get_time_inspt.values())[1])
        end_time.append(list(get_time_inspt.values())[2])
        inspectedcount.append(list(get_time_inspt.values())[3])
        #function.append(list(get_metri_second.values())[0])
        attribute.append(list(get_metri_second.values())[1])
        relativeerror.append(list(get_metri_second.values())[2])
        thresholds.append(list(get_metri_second.values())[3])
        accounts.append(list(value[4].values())[0])
        eventtypes.append(list(value[4].values())[2])
        guid.append(list(value[4].values())[14])
        routerguid.append(list(value[4].values())[15])
        appname.append(list(value[4].values())[17])

    relativeerror_int = [int(val) for val in relativeerror]
    accounts_int = [item for sublist in accounts for item in sublist]

#print(begTime)
#print(perTile)
#print(begTimeSec)
#print(InspCount)
#print(list(value[1].values()))
#print(value)
#print(appName)
#print(RawUntil)
#print(RawSince)
# print(
# list(value[4].values())[17]
# )
getm = list(value[1].values())
getm2 = list(value2[1].values())[1]
get_metri1 = list(value[4].values())[20]
get_metri1_val = list(get_metri1.values())[1]
get_metri2_val = list(get_metri1_val.values())[1][0]
get_metri3_val = list(get_metri2_val.values())[1]

for i in range(0, 2):
    print(i)
A

# print(get_metri1)
# print(get_metri1_val)
# print(get_metri2_val)
#print(get_metri3_val)
#print(getm)
#print(wallclock)


