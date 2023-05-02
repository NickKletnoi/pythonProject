import pandas as pd
import duckdb as dd

waterlevels = pd.DataFrame(
    {
        "time": [
            pd.Timestamp("2023-02-13 01:07:00.000"),
            pd.Timestamp("2023-02-13 01:10:00.000"),
            pd.Timestamp("2023-02-13 01:02:00.000"),
            pd.Timestamp("2023-02-13 01:05:00.000")

        ],
        "StationId": [
               "9014070",
               "9014070",
               "9014070",
               "9014070"

           ],
           "WaterLevel": [72.50, 26.95, 78.97, 23.99]
    }
)

stations = pd.DataFrame({"StationId": ["9014070"]})
times = pd.DataFrame({"time": [pd.Timestamp("01:00:00.000")]})

stations['key'] = 0
times['key'] = 0

stations_list = stations.merge(times, on='key', how='outer')
stations_list = stations_list.drop(['key'], axis=1)

stations_list.rename(columns={'time': 'stations_time'}, inplace=True)
waterlevels.rename(columns={'time': 'waterlevels_time'}, inplace=True)

df = pd.merge_asof(
    left = stations_list.sort_values(['stations_time']),
    right = waterlevels.sort_values(['waterlevels_time']),
    left_on ='stations_time',
    right_on = 'waterlevels_time',
    by='StationId',
    direction='nearest',
    allow_exact_matches=True).sort_values(['waterlevels_time'])

print(df)
# values=['9014070']
# print(df.query("StationId in @values"))

# import duckdb as db
# stationId_Val = '9014070'
# df1 = db.query(f"SELECT StationId, stations_time FROM df where StationId = {stationId_Val}")
#
# print(df1)