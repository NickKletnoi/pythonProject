import pandas as pd

waterlevels = pd.DataFrame(
    {
        "time": [
            pd.Timestamp("2023-02-12 01:07:00.000"),
            pd.Timestamp("2023-02-12 02:10:00.000"),
            pd.Timestamp("2023-02-12 02:02:00.000"),
            pd.Timestamp("2023-02-12 01:03:00.000"),
            pd.Timestamp("2023-02-12 01:06:00.000"),
            pd.Timestamp("2023-02-12 02:08:00.000"),
            pd.Timestamp("2023-02-12 02:01:00.000"),
            pd.Timestamp("2023-02-12 01:04:00.000"),
        ],
        "StationId": [
               "9014070",
               "9014070",
               "9014070",
               "9014070",
               "9014080",
               "9014080",
               "9014080",
               "9014080"
           ],
           "WaterLevel": [72.50, 26.95, 78.97, 23.99, 98.50, 43.99, 65.50, 50.01]
    }
)


stations_x = pd.DataFrame({"StationId": ["9014070", "9014080"]})
time_x = pd.DataFrame({"time": [pd.Timestamp("01:00:00.000"),pd.Timestamp("02:00:00.000")]})

stations_x['key'] = 0
time_x['key'] = 0

stations_list = stations_x.merge(time_x, on='key', how='outer')
stations_list = stations_list.drop(['key'], axis=1)

df = pd.merge_asof(
    stations_list.sort_values(['time']),
    waterlevels.sort_values(['time']),
    on='time',
    by='StationId',
    direction='nearest',
    allow_exact_matches=True).sort_values(['time'])

print(df) #### results
print(stations_list)
print(waterlevels)


