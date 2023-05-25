import os, fnmatch

os.chdir('C:\\Users\\kletn\\PycharmProjects\\PythonProject')
new_path = os.getcwd()
print(new_path)

files = fnmatch.filter(os.listdir(new_path), '*.json')

included_extensions = ['env','json']
file_names = [fn for fn in os.listdir(new_path) if any(fn.endswith(ext) for ext in included_extensions)]

#print(files)
print(file_names)



