# import requests
# import pandas as pd
# import urllib
# from sqlalchemy import create_engine
# import datetime
#
# #2023-02-07 00:36
# #'%m/%d/%Y %H:%M:%S'
#
# #pd.to_datetime(df['Time'], format='%Y-%m-%d %H:%M')
#
#
# # import datetime
# # from pytz import timezone
# # datetime_str = '09/19/2022 13:55:26'
# # new_datetime_str = '2023-02-07 00:36'
# # datetime_object = datetime.datetime.strptime(new_datetime_str, '%Y-%m-%d %H:%M')
# # datetime_object_est = datetime_object.timezone('US/Eastern')
# #
# # print(datetime_object_est)
#
#
# import datetime
# from dateutil.relativedelta import *
# from dateutil.parser import parse
# import pytz
#
# print(parse('March 01, 2019'))
# dt = datetime.now()
# delta = datetime.timedelta(days=30)
# print(dt + relativedelta(months=+1))
# print(dt + relativedelta(months=+1, weeks=+1))
# print(dt + delta)
# print(dt.astimezone(pytz.timezone('US/Arizona')))
#
#
# # tz naive python datetime.datetime object
# naive_python_dt = datetime.datetime(2015, 6, 1, 0)
# ct = datetime.datetime.now()
#
#
# # tz aware python datetime.datetime object
# aware_python_dt = pytz.timezone('US/Mountain').localize(ct)
#
# print(aware_python_dt)

import datetime
import pytz
est_now = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))
now = datetime.datetime.now()
next = now + datetime.timedelta(minutes = 5)
next_time = next.strftime("%Y-%m-%d %H:%M")
current_time = est_now.strftime("%Y-%m-%d %H:%M")
#https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
print(current_time)









# test2 = datetime.datetime.strptime(new_datetime_str,'%Y-%m-%d %H:%M')
#
# test2_est = test2.astimezone(ZoneInfo('America/Eastern'))



#print(datetime_object)# start_time = datetime.datetime.now()
#
# con_str = urllib.parse.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};SERVER=interlake-bi.database.windows.net;DATABASE=ISS_DW;UID=BIAdmin;PWD=sb98D&B(*#$@")
# engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % con_str)
#
# stations = [9063090,9044020,9044030,9044036,9044049,9034052,9014070,9014090,9014098,9075099,9076024,9076027,9076033,9076060,9076070,9099004,9075080,9087031,9087044,9087072,9087057,9099064,9099018,9063079,9063053,9063085,9063063]
# base_url_part1 = 'https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?date=today&station='
# base_url_part2 = '&interval=h&product=water_level&datum=LWD&time_zone=lst_ldt&application=Interlake&units=english&format=json'
#
# def ingest_from_noaa():
#      final_station_list = [base_url_part1 + str(i) + base_url_part2 for i in stations]
#      final_df = pd.DataFrame()
#      for url in final_station_list:
#           response = requests.get(url=url)
#           data = response.json()
#           dfcols = ["StationId", "Time", "WaterLevel"]
#           station = data['metadata']['id']
#           rows = []
#           for d in data['data']:
#                stationid = station
#                time = d['t']
#                waterlevel = d['v']
#                rows.append({"StationId": stationid, "Time": time, "WaterLevel": waterlevel})
#           df = pd.DataFrame(rows, columns=dfcols)
#           #df = df.sort_values('WaterLevel').drop_duplicates('StationId', keep='last')
#           df['day_hour'] = df.apply(lambda r: datetime.datetime(r['Time'].year, r['Time'].month, r['Time'].day, r['Time'].hour, 0), axis=1)
#           df = df.drop('time', axis=1)
#           df = df[df.groupby("StationId","day_hour")['WaterLevel'].transform("median")]
#           final_df = pd.concat([final_df, df], axis=0)
#
#      final_df.to_sql('WaterLevelMeasurements_dev', con=engine, index=False, if_exists='append', schema='dbo')
#      end_time = datetime.datetime.now()
#      execution_time = end_time - start_time
#      print(f"execution time was: {execution_time}")
#      print(final_df)
#      print("Success")
#
# ingest_from_noaa()