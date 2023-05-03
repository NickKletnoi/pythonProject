import json

matrix = [[1,2,3],[4,5],[6,7,8,9]]

flatten_matrix = []
for sublist in matrix:
    for val in sublist:
        flatten_matrix.append(val)

print(flatten_matrix)


flat = [val for sublist in matrix for val in sublist]

print(flat)

##########################################

planets = [['Mercury','Venus','Earth'],['Mars','Jupiter','Saturn'],['Uranus','Neptun','Pluto']]
flat_platn = [planet for sublist in planets for planet in sublist if len(planet) < 6]

print(flat_platn)


