#### List comprehensions ###
## filtering ###

tempatures = [12,32,34,36,34,12,32]
alphanumeric = ["47","abc","21st","n0w4y","test","55123"]
colors = ["red","blue","black"]
models = ["12","12 mini","12 pro"]
###################################################
students_a = ["Anna","Elisa","Tanja","Freja","Frigg"]
students_b = ["Ranja","Natascha","Anna","Tanja"]
###################################################
names = ["John","Mary","Lea"]
surnames = ["Smith","Wonder","Singer"]
ages = ["22","19","25"]
############################################
prices = [22.30,12.00,0.99,1.10]
###########################################
string = 'this is the string of letters that we will count'
def return_next():
    for i in range(10):
        yield i

def convert_to_dollar(euro):
    return round(euro * 1.19,2)

c = list(set([t for t in tempatures if 30 <= t < 35]))
d = [int(s) for s in alphanumeric if s.isdigit()]
e = [(model, color) for model in models for color in colors]
f = [student for student in students_a if student in students_b]
g = [f'{name} {surname} - {age}' for name, surname, age in zip(names, surnames,ages)]
h = [convert_to_dollar(euro) for euro in prices]
j = {c:string.count(c) for c in set(string)}
k = [i for i in return_next()]
l = dict(zip(names, surnames))

print(c)
print(d)
print(e)
print(f)
print(g)
print(h)
print(j)
print(k)
print(l)