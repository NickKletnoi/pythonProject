#!/usr/bin/python
import requests
import pandas as pd
import urllib
from sqlalchemy import create_engine
import datetime
import pyodbc

start_time = datetime.datetime.now()

conn = pyodbc.connect('DRIVER={SQL Server};SERVER=interlake-bi.database.windows.net,1433', user='BIAdmin@interlake-bi', password='sb98D&B(*#$@', database='ISS_DW')
con_str = urllib.parse.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};SERVER=interlake-bi.database.windows.net;DATABASE=ISS_DW;UID=BIAdmin;PWD=sb98D&B(*#$@")
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % con_str)

#stations_lst=[9063090, 9044020, 9044030, 9044036, 9044049, 9034052, 9014070, 9014090, 9014098, 9075099, 9076024,9076027, 9076033, 9076060, 9076070, 9099004, 9075080, 9087031, 9087044, 9087072, 9087057, 9099064,9099018, 9063079, 9063053, 9063085,9063063,9014080, 9063038, 9099090,9075035, 9075065, 9087077, 9087096, 9087023]

stations_lst=[9063063,9075099,9075099,9076027,9076033,9076070,9075080,9087031,9099064,9063079,9063053,9063063,9075065]

base_url_part1 = 'https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?date=latest&station='
base_url_part2_wind = '&product=wind&datum=IGLD&time_zone=gmt&units=english&format=json'
base_url_part2_airtemp = '&product=air_temperature&datum=IGLD&time_zone=gmt&units=english&format=json'
base_url_part2_airpress = '&product=air_pressure&datum=IGLD&time_zone=gmt&units=english&format=json'
base_url_part2_watertemp = '&product=water_temperature&datum=IGLD&time_zone=gmt&units=english&format=json'

def ingest_wind_from_noaa():
    final_station_list = [base_url_part1 + str(i) + base_url_part2_wind for i in stations_lst]
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
    final_df.to_sql('WeatherObservations_Wind_stg', con=engine, index=False, if_exists='append', schema='dbo')

    end_time = datetime.datetime.now()
    execution_time = end_time - start_time
    print(f"execution time was: {execution_time}")
    print(final_df.to_string())
    print("Success")


def ingest_airtemp_from_noaa():
    final_station_list = [base_url_part1 + str(i) + base_url_part2_airtemp for i in stations_lst]
    final_df = pd.DataFrame()
    for url in final_station_list:
        response = requests.get(url=url)
        data = response.json()
        dfcols = ["StationId", "ObservationTime", "AirTemperature"]
        station = data['metadata']['id']
        rows = []
        for d in data['data']:
            stationid = station
            time = d['t']
            airtemp = d['v']
            rows.append({"StationId": stationid, "ObservationTime": time, "AirTemperature": airtemp})
        df = pd.DataFrame(rows, columns=dfcols)
        final_df = pd.concat([final_df, df], axis=0)

    final_df = final_df.astype({'StationId': 'int64','ObservationTime': 'datetime64[ns]','AirTemperature': 'float64'})
    final_df.to_sql('WeatherObservations_AirTemp_stg', con=engine, index=False, if_exists='append', schema='dbo')

    end_time = datetime.datetime.now()
    execution_time = end_time - start_time
    print(f"execution time was: {execution_time}")
    print(final_df.to_string())
    print("Success")


def ingest_watertemp_from_noaa():
    final_station_list = [base_url_part1 + str(i) + base_url_part2_watertemp for i in stations_lst]
    final_df = pd.DataFrame()
    for url in final_station_list:
        response = requests.get(url=url)
        data = response.json()
        dfcols = ["StationId", "ObservationTime", "WaterTemperature"]
        station = data['metadata']['id']
        rows = []
        for d in data['data']:
            stationid = station
            time = d['t']
            airtemp = d['v']
            rows.append({"StationId": stationid, "ObservationTime": time, "WaterTemperature": airtemp})
        df = pd.DataFrame(rows, columns=dfcols)
        final_df = pd.concat([final_df, df], axis=0)

    final_df = final_df.astype({'StationId': 'int64','ObservationTime': 'datetime64[ns]','WaterTemperature': 'float64'})
    final_df.to_sql('WeatherObservations_WaterTemp_stg', con=engine, index=False, if_exists='append', schema='dbo')

    end_time = datetime.datetime.now()
    execution_time = end_time - start_time
    print(f"execution time was: {execution_time}")
    print(final_df.to_string())
    print("Success")

def ingest_airpress_from_noaa():
    final_station_list = [base_url_part1 + str(i) + base_url_part2_airpress for i in stations_lst]
    final_df = pd.DataFrame()
    for url in final_station_list:
        response = requests.get(url=url)
        data = response.json()
        dfcols = ["StationId", "ObservationTime", "AirPressure"]
        station = data['metadata']['id']
        rows = []
        for d in data['data']:
            stationid = station
            time = d['t']
            airpress = d['v']
            rows.append({"StationId": stationid, "ObservationTime": time, "AirPressure": airpress})
        df = pd.DataFrame(rows, columns=dfcols)
        final_df = pd.concat([final_df, df], axis=0)

    final_df = final_df.astype({'StationId': 'int64','ObservationTime': 'datetime64[ns]','AirPressure': 'float64'})
    final_df.to_sql('WeatherObservations_AirPressure_stg', con=engine, index=False, if_exists='append', schema='dbo')

    end_time = datetime.datetime.now()
    execution_time = end_time - start_time
    print(f"execution time was: {execution_time}")
    print(final_df.to_string())
    print("Success")

def final_table_assemble():
    conn.execute("exec dbo.sp_weatherobs_insert")
    conn.commit()

ingest_wind_from_noaa()
ingest_airtemp_from_noaa()
ingest_watertemp_from_noaa()
ingest_airpress_from_noaa()
final_table_assemble()

