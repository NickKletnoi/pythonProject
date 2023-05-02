import azure.functions as func
import requests
import pandas as pd
import urllib
from sqlalchemy import create_engine
import datetime

start_time = datetime.datetime.now()

con_str = urllib.parse.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};SERVER=interlake-bi.database.windows.net;DATABASE=ISS_DW;UID=BIAdmin;PWD=sb98D&B(*#$@")
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % con_str)


stations_lst = [9063090, 9044020, 9044030, 9044036, 9044049, 9034052, 9014070, 9014090, 9014098, 9075099, 9076024,
                9076027, 9076033, 9076060, 9076070, 9099004, 9075080, 9087031, 9087044, 9087072, 9087057, 9099064,
                9099018, 9063079, 9063053, 9063085, 9063063]

#stations_lst = [9014070,9014080,9014081,9014090,9014098,9034052,9044020,9044030,9044035,9044036,9044049,9063038,9063053,9063063,9063079,9063085,9063090,9075035,9075057,9075065,9075079,9075080,9075099,9076024,9076027,9076032,9076033,9076060,9076070,9087031,9087044,9087057,9087072,9087077,9087096,9099004,9099016,9099018,9099064,9099090]


times_df = pd.DataFrame({"Time": [
    pd.Timestamp("01:00:00.000"),
    pd.Timestamp("02:00:00.000"),
    pd.Timestamp("03:00:00.000"),
    pd.Timestamp("04:00:00.000"),
    pd.Timestamp("05:00:00.000"),
    pd.Timestamp("06:00:00.000"),
    pd.Timestamp("07:00:00.000"),
    pd.Timestamp("08:00:00.000"),
    pd.Timestamp("09:00:00.000"),
    pd.Timestamp("10:00:00.000"),
    pd.Timestamp("11:00:00.000"),
    pd.Timestamp("12:00:00.000"),
    pd.Timestamp("13:00:00.000"),
    pd.Timestamp("14:00:00.000"),
    pd.Timestamp("15:00:00.000"),
    pd.Timestamp("16:00:00.000"),
    pd.Timestamp("17:00:00.000"),
    pd.Timestamp("18:00:00.000"),
    pd.Timestamp("19:00:00.000"),
    pd.Timestamp("20:00:00.000"),
    pd.Timestamp("21:00:00.000"),
    pd.Timestamp("22:00:00.000"),
    pd.Timestamp("23:00:00.000"),
    pd.Timestamp("00:00:00.000")
]})

# Historical Jan
# https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?begin_date=20230101&end_date=20230131&station=9063090&interval=h&product=water_level&datum=LWD&time_zone=lst_ldt&application=Interlake&units=english&format=json

# Historical Feb
# https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?begin_date=20230201&end_date=20230228&station=9063090&interval=h&product=water_level&datum=LWD&time_zone=lst_ldt&application=Interlake&units=english&format=json
# base_url_part1 = 'https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?begin_date=20230101&end_date=20230131&station='

#base_url_part1 = 'https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?begin_date=20230101&end_date=20230131&station='
#base_url_part2 = 'interval=h&product=water_level&datum=LWD&time_zone=lst_ldt&application=Interlake&units=english&format=json'

base_url_part1 = 'https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?date=today&station='
base_url_part2 = '&interval=h&product=water_level&datum=LWD&time_zone=lst_ldt&application=Interlake&units=english&format=json'

#def main(mytimer: func.TimerRequest) -> None:
def main(req: func.HttpRequest) -> func.HttpResponse:
    final_station_list = [base_url_part1 + str(i) + base_url_part2 for i in stations_lst]
    final_df = pd.DataFrame()
    for url in final_station_list:
        response = requests.get(url=url)
        data = response.json()
        dfcols = ["StationId", "Time", "WaterLevel"]
        station = data['metadata']['id']
        rows = []
        for d in data['data']:
            stationid = station
            time = d['t']
            waterlevel = d['v']
            rows.append({"StationId": stationid, "Time": time, "WaterLevel": waterlevel})
        df = pd.DataFrame(rows, columns=dfcols)
        final_df = pd.concat([final_df, df], axis=0)

    stations_df = pd.DataFrame(stations_lst, columns=['StationId'])
    stations_df = stations_df.astype({'StationId': 'int64'})

    stations_df['key'] = 0
    times_df['key'] = 0

    stations_final_df = stations_df.merge(times_df, on='key', how='outer')
    stations_final_df = stations_final_df.drop(['key'], axis=1)
    stations_final_df.rename(columns={'Time': 'StationsTime'}, inplace=True)

    final_df = final_df.astype({'WaterLevel': 'float64', 'Time': 'datetime64[ns]', 'StationId': 'int64'})
    final_df = final_df.fillna({'WaterLevel': '0.00'}, inplace=True)
    #final_df['WaterLevel'] = final_df['WaterLevel'].apply(lambda x: (x * 12))
    #final_df.rename(columns={'Time': 'WaterlevelsTime'}, inplace=True)

    # df_ff = pd.merge_asof(
    #     left=stations_final_df.sort_values(['StationsTime']),
    #     right=final_df.sort_values(['WaterlevelsTime']),
    #     left_on='StationsTime',
    #     right_on='WaterlevelsTime',
    #     by='StationId',
    #     direction='nearest',
    #     allow_exact_matches=True).sort_values(['WaterlevelsTime'])

    #df_ff.rename(columns={'WaterlevelsTime': 'Time'}, inplace=True)
    #df_ff = df_ff.drop(['StationsTime'], axis=1)
    #df_ff.to_sql('STG_WaterLevelMeasurements', con=engine, index=False, if_exists='append', schema='dbo')

    end_time = datetime.datetime.now()
    execution_time = end_time - start_time
    print(f"execution time was: {execution_time}")
    print(final_df.to_string())
    print("Success")

    return func.HttpResponse(f"Successfully executed  in  {execution_time} seconds")

main({ "name": "Azure" })

