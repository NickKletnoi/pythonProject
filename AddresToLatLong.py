import pandas as pd
from geopy.geocoders import Nominatim
import xlsxwriter
import locator
from geopy.extra.rate_limiter import RateLimiter
df = pd.read_csv('data/Addresses.csv')

#print(df.head(10))

# 1 - conveneint function to delay between geocoding calls
geocode = RateLimiter(locator.geocode, min_delay_seconds=1)
# 2- - create location column
df['location'] = df['ADDRESS'].apply(geocode)
# 3 - create longitude, laatitude and altitude from location column (returns tuple)
df['point'] = df['location'].apply(lambda loc: tuple(loc.point) if loc else None)
# 4 - split point column into latitude, longitude and altitude columns
df[['latitude', 'longitude', 'altitude']] = pd.DataFrame(df['point'].tolist(), index=df.index)

print(df.head())