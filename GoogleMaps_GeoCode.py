import pandas as pd
import googlemaps
from datetime import datetime, timedelta

gmaps = googlemaps.Client(key="AIzaSyBe-50GHt8aJC75shtyhsw7-Ca2BXIsAlM")
df = pd.read_csv('data/zomato1.csv',encoding="ISO-8859-1")
#/dbfs/FileStore/tables/zomato1.csv

def geocode(add):
    g = gmaps.geocode(add)
    lat = g[0]["geometry"]["location"]["lat"]
    lng = g[0]["geometry"]["location"]["lng"]
    return (lat, lng)

def formatted_addr(addr):
    g = gmaps.geocode(addr)
    formatted_addr = g[0]["formatted_address"]
    return formatted_addr


#df['geocoded'] = df['Address'].apply(geocode)
#df['formatted_address'] = df['Address'].apply(formatted_addr)
df['geocoded'] = [geocode(x) for x in df['Address']]
df['formatted_address'] = [formatted_addr(x) for x in df['Address']]


ft_addr = df[['formatted_address']]
geo_df = df[['Location_ID','formatted_address','geocoded']]
print(geo_df.to_string())

source_location = formatted_addr(ft_addr.iloc[3,0])
destination_location = formatted_addr(ft_addr.iloc[5,0])

directions_result = gmaps.directions(source_location,
                                     destination_location,
                                     mode="transit",
                                     arrival_time=datetime.now() + timedelta(minutes=0.5))
print(source_location)
print(destination_location)
print(directions_result)

### plot routes ###

locations = ["47236 Middle Bluff Pl., Sterling, VA 20165",
          "45575 Dulles Eastern Plaza Ste 188, Sterling, VA 20166"]

markers = ["color:blue|size:mid|label:" + chr(65+i) + "|"
                   + r for i, r in enumerate(locations)]

result_map = gmaps.static_map(
                 center=locations[0],
                 scale=2,
                 zoom=12,
                 size=[640, 640],
                 format="jpg",
                 maptype="roadmap",
                 markers=markers,
                 path="color:0x0000ff|weight:2|" + "|".join(locations))


with open("driving_route_map.jpg", "wb") as img:
    for chunk in result_map:
        img.write(chunk)