#!/usr/bin/python
import requests
import pandas as pd
import urllib
from sqlalchemy import create_engine
import datetime

start_time = datetime.datetime.now()

con_str = urllib.parse.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};SERVER=interlake-bi.database.windows.net;DATABASE=ISS_DW;UID=BIAdmin;PWD=sb98D&B(*#$@")
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % con_str)

#stations_lst=[9063090, 9044020, 9044030, 9044036, 9044049, 9034052, 9014070, 9014090, 9014098, 9075099, 9076024,9076027, 9076033, 9076060, 9076070, 9099004, 9075080, 9087031, 9087044, 9087072, 9087057, 9099064,9099018, 9063079, 9063053, 9063085,9063063,9014080, 9063038, 9099090,9075035, 9075065, 9087077, 9087096, 9087023]

stations_lst=[9063063]

base_url_part1 = 'https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?date=latest&station='
base_url_part2 = '&product=wind&datum=IGLD&time_zone=gmt&units=english&format=json'

def ingest_from_noaa():
    final_station_list = [base_url_part1 + str(i) + base_url_part2 for i in stations_lst]
    final_df = pd.DataFrame()
    for url in final_station_list:
        response = requests.get(url=url)
        data = response.json()
        dfcols = ["StationId", "ObservationTime", "WindSpeed","WindDirection"]
        station = data['metadata']['id']
        rows = []
        for d in data['data']:
            stationid = station
            time = d['t']
            wind_speed = d['s']
            wind_direction = d['dr']
            rows.append({"StationId": stationid, "ObservationTime": time, "WindSpeed": wind_speed, "WindDirection": wind_direction})
        df = pd.DataFrame(rows, columns=dfcols)
        final_df = pd.concat([final_df, df], axis=0)

    final_df = final_df.astype({'StationId': 'int64','ObservationTime': 'datetime64[ns]','WindSpeed': 'float64','WindDirection': 'string'})
    final_df.to_sql('WeatherObservations_stg', con=engine, index=False, if_exists='append', schema='dbo')

    end_time = datetime.datetime.now()
    execution_time = end_time - start_time
    print(f"execution time was: {execution_time}")
    print(final_df.to_string())
    print("Success")


ingest_from_noaa()
