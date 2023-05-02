from shapely.geometry import Point
import geopandas as gpd
from geopandas import GeoDataFrame
import plotly.express as px
import pandas as pd
from geopy.geocoders import Nominatim

geolocator = Nominatim(user_agent="Nick_Kletnoi")

top5Hospitals = [
        '8260 Atlee Road, Mechanicsville, VA 23116',
        '727 North Main Street, Emporia, VA 23847',
        '800 Oak Street, Farmville, VA 23901'
       ]

df = pd.DataFrame(top5Hospitals, columns=['hospitals'])
df['location'] = df['hospitals'].apply(lambda x: geolocator.geocode(x))
df['address'] = df['location'].apply(lambda x: x.address)
df['lat'] = df['location'].apply(lambda x: x.latitude)
df['lon'] = df['location'].apply(lambda x: x.longitude)


print(df)
#df.to_csv('data/hospitals.csv')


#df = pd.read_csv("Long_Lats.csv", delimiter=',', skiprows=0, low_memory=False)

# geometry = [Point(xy) for xy in zip(df['lon'], df['lat'])]
# gdf = GeoDataFrame(df, geometry=geometry)
#
# #this is a simple map that goes with geopandas
# world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
# gdf.plot(ax=world.plot(figsize=(10, 6)), marker='o', color='red', markersize=15);

fig = px.scatter_geo(df,lat='lat',lon='lon', hover_name='location')
fig.update_layout(title = 'World map', title_x=0.5)
fig.show()
