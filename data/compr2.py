sample_data = [
    {"UserId": 3, "name": "rahul"},
    {"UserId": 2, "name": "ron"},
    {"UserId": 1, "name": "nick"}
]
names = [name.get("name") for name in sample_data]
userIds = [user.get("UserId") for user in sample_data]

compl_dict = {user.get("UserId"): user.get("name") for user in sample_data}
print(compl_dict)
print(names)
['rahul', 'ron', 'nick']
print(userIds)
[1, 2, 3]

new_names ={a:b for (a,b) in zip (userIds,names)}
sorted_keys = sorted(new_names.items(), key=lambda item: item[0])
new_names_sorted_dict = {k: v for k, v in sorted_keys}
print(new_names)
print(new_names_sorted_dict)


import string
import re
s = ',Hello, World.'
s1 = ',Hello, World.'
s2 = ',Hello, World.!'
s = ''.join(c for c in s if c not in string.punctuation)
s1 = ''.join(filter(lambda x: x not in string.punctuation,s))
s2 = re.sub(r'[.,"\'-?:!;]','',s2)
print(s)
print(s1)
print(s2)
'Hello World'


class ComplexNumber2:
    def __init__(self):
        pass

    def get_data(self,k):
        print(k)

num2 = ComplexNumber2()

num2.get_data(4)

