import requests

from geopy.geocoders import Nominatim
geolocator = Nominatim(user_agent='bsbsm_app')


def get_nominatim_geocode(address):
    try:
        location = geolocator.geocode(address)
        return location.raw['lon'], location.raw['lat']
    except Exception as e:
        # print(e)
        return None, None

def get_geocode(address):
    long, lat = get_nominatim_geocode(address)
    return long, lat


address = "50TH ST S, NEW YORK, NY"

long, lat = get_geocode(address)

print(lat,long)