from datetime import timedelta, datetime, date
import dateutil.relativedelta

# current time
date_and_time = datetime.now()
date_only = date.today()
time_only = datetime.now().time()

# calculate date and time
result = date_and_time - timedelta(hours=26, minutes=25, seconds=10)
# calculate dates: years (-/+)
result_10_years = date_only - dateutil.relativedelta.relativedelta(years=10)
# months
result_10_months = date_only - dateutil.relativedelta.relativedelta(months=10)
# week
results_1_week = date_only - dateutil.relativedelta.relativedelta(weeks=1)
# days
result_10_days = date_only - dateutil.relativedelta.relativedelta(days=10)
# calculate time
result = date_and_time - timedelta(hours=26, minutes=25, seconds=10)
result.time()
print(result_10_years)
print(result_10_months)
print(results_1_week)
print(result_10_days)

