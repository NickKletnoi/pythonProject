import pandas as pd
import googlemaps
gmaps_key = googlemaps.Client(key="AIzaSyBe-50GHt8aJC75shtyhsw7-Ca2BXIsAlM")
df = pd.read_csv('data/zomato1.csv',encoding="ISO-8859-1")

def geocode(add):
    g = gmaps_key.geocode(add)
    lat = g[0]["geometry"]["location"]["lat"]
    lng = g[0]["geometry"]["location"]["lng"]
    return (lat, lng)

def formatted_addr(addr):
    g = gmaps_key.geocode(addr)
    formatted_addr = g[0]["formatted_address"]
    return formatted_addr

df['geocoded'] = df['Address'].apply(geocode)

new_df = df[['Location_ID','Address','geocoded']]

print(new_df.to_string())

