import  json
cities = ['Austin','Tacoma','Topeka','Sacramento','Charlotte']
temps = {city: [0 for _ in range(7)] for city in cities}

print(temps.get('Topeka'))

rollNumbers =[122,233,353,456]
names = ['alex','bob','can', 'don']
NewDictionary={ i:j for (i,j) in zip (names,rollNumbers)}
print(NewDictionary)

### for a string ####
person= '{"name": "bob", "languages": ["English","French"]}'
person_dict = json.loads(person)
print(person_dict)
print(json.dumps(person_dict, indent=4))
print(person_dict.get('languages'))

#### for a file read####
with open('person.json') as f:
    data = json.load(f)
print(data)

### for a file write ###
with open('newperson.json','w') as f:
    json.dump(data, f)

transposed = []
matrix = [[1, 2, 3, 4], [4, 5, 6, 8]]
matrix2 = [[1, 2], [3,4], [5,6], [7,8]]

# for i in range(len(matrix[0])):
#     transposed_row = []
#
#     for row in matrix:
#         transposed_row.append(row[i])
#     transposed.append(transposed_row)

#print(transposed)
transpose1 = [[row[i] for row in matrix] for i in range(len(matrix[0]))]
print(transpose1)
transpose2 = [[row[i] for row in matrix2] for i in range(len(matrix2[0]))]
print(transpose2)

# transpose = [[r[i] for r in matrix1] for i in matrix1]
# print (transpose)

from datetime import datetime

timestamps = ['30:02:17:36',
              '26:07:44:25','25:19:30:38','25:07:40:47','24:18:29:05','24:06:13:15','23:17:36:39',
              '23:00:14:52','22:07:04:33','21:15:42:20','21:04:27:53',
              '20:12:09:22','19:21:46:25']
timestamps_dt = [datetime.strptime(d, '%d:%H:%M:%S') for d in timestamps]

print(timestamps_dt)


txns = [1.09, 23.56, 57.84, 4.56, 6.78]
TAX_RATE = .08
def get_price_with_tax(txn):
     return txn * (1 + TAX_RATE)
final_prices = [get_price_with_tax(i) for i in txns]
final_prices2 = [price for i in txns if (price := get_price_with_tax(i)) < 7]

print(final_prices2)

numbers = [1,34,5,8,10,12,3,90,70,70,90]
unique_even_numbers = {number for number in numbers if number%2 == 0}
r = sorted(unique_even_numbers)
print(r)


data = [10, 20, 30, 40]
def greater_10(x):
    if x > 10:
        return x
newval = [y for x in data if (y := greater_10(x)) is not None]
print(newval)


########################################################
###### Splitting the values of a list ###############
names = ["Dick Price", "Ben Dover"]
new_names={(n := name.lower()): n.split() for name in names}
new_names2 = [name2 for name2 in new_names.values()]
print(new_names2)

sample_data = [
    {"UserId": 3, "name": "rahul"},
    {"UserId": 2, "name": "ron"},
    {"UserId": 1, "name": "nick"}
]
names = [name.get("name") for name in sample_data]
userIds = [user.get("UserId") for user in sample_data]
print(names)
print(userIds)



