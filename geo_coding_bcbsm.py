from geopy.geocoders import Nominatim

#address='380 New York St, Redlands, CA 92373'
address = '2235 Staples Mill Road Suite 104, Richmond, VA 23230'
#address = '47236 Mid
# dle Bluff Place, Sterling, VA 20165'
geolocator = Nominatim(user_agent="Nick_Kletnoi")
location = geolocator.geocode(address)
print(location.address)
print((location.latitude, location.longitude))
#Barcelona, Barcelonès, Barcelona, Catalunya, 08001, España
#(41.3828939, 2.1774322)


