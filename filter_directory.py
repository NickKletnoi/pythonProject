import os, fnmatch

#files = fnmatch.filter(os.listdir('C:\\Users\\kletn\\PycharmProjects\\PythonProject'), '*.json')

included_extensions = ['env','json']
file_names = [fn for fn in os.listdir('C:\\Users\\kletn\\PycharmProjects\\PythonProject')
                                if any(fn.endswith(ext)
                                       for ext in included_extensions)]

print(file_names)


#####################################################################################################
## recursive search ##################
# pattern = '*.json'
# for dirpath, dirnames, filenames in os.walk(new_path):
#
#     if not filenames:
#         continue
#
#     pythonic_files = fnmatch.filter(filenames, pattern)
#     if pythonic_files:
#         for file in pythonic_files:
#             print('{}/{}'.format(dirpath, file))



from pathlib import Path

# import os
# accepted_extensions = ["json"]
# filenames1 = [fn for fn in os.listdir('C:\\Users\\kletn\\PycharmProjects\\PythonProject') if fn.split(".")[-1] in accepted_extensions]
# print(filenames1)

#os.mkdir('test')
#os.rename('test','new_one')
#os.remove("myfile.txt")
#os.rmdir("mydir")
# import shutil
#
# # delete "mydir" directory and all of its contents
# shutil.rmtree("mydir")
