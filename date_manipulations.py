import datetime
import pytz
est_now = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))
now = datetime.datetime.now()
next_day = est_now + datetime.timedelta(days = 1)
previous_day = est_now + datetime.timedelta(days = -1)
next_time = next_day.strftime("%Y-%m-%d %H:%M")
current_time = est_now.strftime("%Y-%m-%d %H:%M")
next_day_str = next_day.strftime("%Y%m%d")
previous_day_str = previous_day.strftime("%Y%m%d")
#https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
print("tomorrow is: " + next_day_str)
print("yesterday was: " + previous_day_str)