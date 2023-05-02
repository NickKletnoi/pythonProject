import datetime
import pytz
est_now = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))
now = datetime.datetime.now()
next_day = est_now + datetime.timedelta(days = 1)
previous_day = est_now + datetime.timedelta(days = -1)
next_time = next_day.strftime("%Y-%m-%d %H:%M")
current_time = est_now.strftime("%Y-%m-%d %H:%M")
next_day_str = next_day.strftime("%Y-%m-%d")
previous_day_str = previous_day.strftime("%Y-%m-%d")
#https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
print("tomorrow is: " + next_day_str)
print("yesterday was: " + previous_day_str)


#
#
#
#
#
# # Driver Code
# days = [12,13,14]
# stations = [123,234,567]
# base_http = 'http//noadays='
# staions_htp = '&stations='

# for station in stations:
#     for day in days:
#         print(base_http+str(day)+staions_htp+str(station))

#stations_list = [base_http+str(day)+staions_htp+str(station) for station in stations for day in days]

#print(stations_list)

###newbie exact answer desired (Python v3):
###=================================

# cars = {'A':{'speed':70,
#         'color':2},
#         'B':{'speed':60,
#         'color':3}}
#
#
# for keys, values in  reversed(sorted(cars.items())):
#     print(keys)
#     for keys,values in sorted(values.items()):
#         print(keys," : ", values)

