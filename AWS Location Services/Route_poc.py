# aws location \
#     calculate-route-matrix \
#         --calculator-name MyRouteCalculator \
#         --departure-positions "[[-83.028056,42.579722]]" \
#         --destination-positions "[[-83.045833,42.331389]]"

#-83.028056,42.579722
#-83.045833,42.331389
dep_lat = 42.579722
dep_lon = -83.028056
######################
dest_lat = 42.331389
dest_lon = -83.045833

def route(dep_lat,dep_lon,dest_lat,dest_lon):
    dep_val = []
    dep_val.append([dep_lon,dep_lat])
    dest_val = []
    dest_val.append([dest_lon, dest_lat])
    return dep_val, dest_val

departure,destination = route(dep_lat,dep_lon,dest_lat,dest_lon)

print(departure)
print(destination)