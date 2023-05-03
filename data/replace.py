import re
s = ',Hello, World.'
s1 = ',Hello, World.'
s2 = ',Hello, World.!'
s3 = '-Hello-World.!'
s4 = '-Hello-World.!'
s5 = 'I like tortilla.'
s6 = 'dknnuisocrapplksjdl'
s = ''.join(c for c in s if c not in string.punctuation)
s1 = ''.join(filter(lambda x: x not in string.punctuation,s))
s2 = re.sub(r'[.,"\'-?:!;]','',s2)
s3 = re.sub(r'[-]','',s3)
##################################################################
######### RegEx with List Comprehension Example ##################
##################################################################
list = ['a cat', 'a dog', 'a yacht', 'cats']
regex_cat = re.compile(".*cat.*")
mycats = [m.group(0) for l in list for m in [regex_cat.search(l)] if m]
print(mycats)
#################################################################

regex_result = re.search(r'like (?P<food>\w+)', s5)
print(s)
print(s1)
print(s2)
print(s3)
print('s6 is:' + s6)
print(regex_result.groupdict())
################################################

fh = open(r"email_sample.txt", "r").read()

for fr_name in re.findall("From:.*", fh):
    print(fr_name)

for email in re.findall(r"\w\S*@.*\.com", fh):
    print(email)

#er part at the end
words = ['surrender', 'up', 'newer', 'do', 'ear', 'eel', 'pest']
fil = [w for w in words if re.search(r'er\Z', w)]
fil2 = [w for w in words if re.search(r'\Ap', w)]
print(fil)
print(fil2)
#['surrender', 'newer']

# whole word par with optional s at start and optional e at end
list_find1 = re.findall(r'\bs?pare?\b', 'par spar apparent spare part pare')
#['par', 'spar', 'spare', 'pare']
print(list_find1)

# split based on one or more digit characters
list_split = re.split(r'\d+', 'Sample123string42with777numbers')
#['Sample', 'string', 'with', 'numbers']
print(list_split)
new_string = '-'.join(list_split)
print(new_string)

pattern_str12 = r'[\d\s]+'
# split based on digit or whitespace characters
list_split2 = re.split(pattern_str12, '**1\f2\n3star\t7 77\r**')
#['**', 'star', '**']
print(list_split2)

pattern_str11 = r'\A([^:]+:){2}'
# remove first two columns where : is delimiter
list_split3 = re.sub(pattern_str11, r'', 'foo:123:bar:baz', count=1)
'bar:baz'
print(list_split3)

# replace 'par' only at start of word
list_repl4 = re.sub(r'\bpar', r'X', 'par spar apparent spare part')
print(list_repl4)

pattern_str0 = r'[0-9]+'
# numbers < 350 from a strring
m_iter = re.finditer(pattern_str0, '45 349 651 593 4 204')
fil_num_list =  [m[0] for m in m_iter if int(m[0]) < 350]
#['45', '349', '4', '204']
print(fil_num_list)

######## perform a math operation on each element of a list return list ############
numbers2 = [1,2,3,4,5]
def fact_num(n):
    return str(n+2)
file_num_list6 = [fact_num(x) for x in numbers2]
print(file_num_list6)

####### perform math operatio on each value element of a string return string
def fact_num3(n):
    return str(int(n[0])+2)
numbers3 = '1 2 3 4 5'
pattern_str2 = r'\d+'
fact_string_7 =re.sub(pattern_str2, fact_num3, numbers3)
print(fact_string_7)

# same as above only using lambda with an added condition for filtering the value to a math value
fact_string_8 = re.sub(pattern_str2, lambda m: str(int(m[0])+2) if (int(m[0])>3) else m[0], numbers3)
print('######################################')
print(fact_string_8)
pattern_str = r"(\b[A-Z]+\b).+(\b\d+)"
target_string = "The price of PINEAPPLE ice cream is 20"
result = re.search(pattern_str, target_string)
# Extract matching values of all groups
print(result.groups())
# Output ('PINEAPPLE', '20')
# Extract match value of group 1
print(result.group(1))
# Output 'PINEAPPLE'
# Extract match value of group 2
print(result.group(2))
# Output 20
#################### with compiler ###########################

# Target String one
str1 = "Emma's luck numbers are 251 761 231 451"

# pattern to find three consecutive digits
string_pattern = r"\d{3}"
# compile string pattern to re.Pattern object
regex_pattern = re.compile(string_pattern)

# print the type of compiled pattern
print(type(regex_pattern))
# Output <class 're.Pattern'>

# find all the matches in string one
result = regex_pattern.findall(str1)
print(result)
# Output ['251', '761', '231', '451']

# Target String two
str2 = "Kelly's luck numbers are 111 212 415"
# find all the matches in second string by reusing the same pattern
result = regex_pattern.findall(str2)
print(result)
# Output ['111', '212', '415']

pattern_password = r'^(?=.*[a-z])(?=.*[A-Z])(?=.*\d).{6,12}$'
regex_pattern_password = re.compile(pattern_password)
# 6 to 12, one upper, one lower,  one digit, and other characters
password_value = 'B1Program'
result_pass = bool(re.search(pattern_password, password_value))
result_pass2 = bool(regex_pattern_password.search(password_value))
print(str(result_pass))
print(str(result_pass2))

pattern_url = r"^(http|https|ftp)\:[\/]{2}([a-zA-z0-9\-\.]+\.[a-zA-Z]{2,4})(:[0-9]+)?\/?([a-zA-Z0-9\-\._\?\,\'\/\\\+&amp;%\$#\=~]*)"
regex_pattern_url = re.compile(pattern_url)
# 6 to 12, one upper, one lower,  one digit, and other characters
url_value = 'http://wwww.sitepoint.commnkj:80'
result_url = bool(re.search(pattern_url, url_value))
result_url2 = bool(regex_pattern_url.search(url_value))
print(str(result_url))
print(str(result_url2))


##########Pattern CC #####################################

pattern_cc = r"(\b(?:4[0-9]{12}(?:[0-9]{3})?|(?:5[1-5][0-9]{2}|2720|27[01][0-9]|2[3-6][0-9]{2}|22[3-9][0-9]|222[1-9])[0-9]{12}|3[47][0-9]{13}|3(?:0[0-5]|[68][0-9])[0-9]{11}|6(?:011|5[0-9]{2})[0-9]{12}|(?:2131|1800|35[0-9]{3})[0-9]{11})\b)"

########### Resulting output is a string ###################
############################################################
pattern_str8 = r"^(.*?)soc(.*?)$"
pattern_str9 = r"^.*?soc.*?$"
target_string8 = 'dknnuisocrapplksjdl'
result8 = re.search(pattern_str8, target_string8)

if (match := re.search(pattern_str9, target_string8)) is not None:
    print('pattern match is: ' + str(match.group(0)))

######## Resulting output is a list #######################
##########################################################
string_pattern_soc = r"^.*?soc.*?$"
regex_pattern_soc = re.compile(string_pattern_soc)
result_soc = regex_pattern_soc.findall(target_string8)
print(result_soc)


text = '678-908-0989'

phoneRegex = re.compile(r'''(
(\d{3}|\(\d{3}\))? #area code
(?:5[1-5][0-9]{2}|2720|27[01][0-9]|2[3-6][0-9]{2}|22[3-9][0-9]|222[1-9])         #separator
[0-9]{12}|3[47][0-9]{13}|3           #first 3 digits
(?:0[0-5]|[68][0-9])        #separator
(\d{4})            #last 4 digits
(\s*(ext|x|ext.)\s*(\d{2,5}))? #extention
)''',re.VERBOSE)

CreditCardRegex = re.compile(r'''(
(\b(?:4[0-9]{12}(?:[0-9]{3})?| #area code
(\s|-|\.)?         #separator
(\d{3})            #first 3 digits
(\s|-|\.)          #separator
(\d{4})            #last 4 digits
(\s*(ext|x|ext.)\s*(\d{2,5}))? #extention
)''',re.VERBOSE)



emailRegex = re.compile(r'''(
[a-zA-Z0-9._%+-]+  #username
@                  #@ symbol
[a-zA-Z0-9.-]+     #domain name
(\.[a-zA-Z]{2,4})                         
 )''',re.VERBOSE)

matches_org = []
matches_mod = []

for group in phoneRegex.findall(text):
    phoneNum_mod = '.'.join([group[1], group[3],group[5]])
    phoneNum_org = group[0]
    matches_org.append(phoneNum_org)
    matches_mod.append(phoneNum_mod)

print(matches_org)
print(matches_mod)


patterns = ['this', 'that']
text = 'Does this text match the pattern?'

for pattern in patterns:
    if re.search(pattern,  text):
        print('found a match!')
        yes = re.search(pattern,  text)
        print(yes.group(0))

################### Match the values in the list with a given string ###########
###############################################################################

myList = ['test;cow', 'one', 'two', 'three', 'cow.', 'cow', 'acow']
myString = 'cow'
regex = r'\b' + re.escape(myString) + r'\b'
indices = [x for i, x in enumerate(myList) if re.search(regex, x)]
print(indices)


regex2 = r'\W*' + re.escape(myString) + r'\W*'
indices2 = [x for i, x in enumerate(myList) if re.match(regex2, x)]
print(indices2)

list = ['a cat', 'a dog', 'a yacht', 'cats']
regex_cat = re.compile(".*cat.*")
mycats = [m.group(0) for l in list for m in [regex_cat.search(l)] if m]
print(mycats)
#['a cat', 'cats']

mynewcats = [c for c in list if re.search(r'.*cat.*', c)]
print(mynewcats)
#['a cat', 'cats']

myothercat = [c for c in list if c.__contains__('cat')]
print(myothercat)
#['a cat', 'cats']

bigcat = [word.capitalize()
     for line in list
     for word in line.split() if
     word.startswith('cat')]
print(bigcat)
#['Cat', 'Cats']


import functools
import time

def timer(func):
    """Print the runtime of the decorated function"""
    @functools.wraps(func)
    def wrapper_timer(*args, **kwargs):
        start_time = time.perf_counter()    # 1
        value = func(*args, **kwargs)
        end_time = time.perf_counter()      # 2
        run_time = end_time - start_time    # 3
        print(f"Finished {func.__name__!r} in {run_time:.4f} secs")
        return value
    return wrapper_timer

@timer
def waste_some_time(num_times):
    for _ in range(num_times):
        sum([i**2 for i in range(10000)])

waste_some_time(999)