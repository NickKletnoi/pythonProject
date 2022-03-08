import os
list_of_lines = ['Another line to prepend', 'Second Line to prepend',  'Third Line to prepend', 'Forth Line to prepend']
#################################################################
def prepend_multiple_lines(file_name, list_of_lines):
    dummy_file = file_name + '.bak'
    with open(file_name, 'r') as read_obj, open(dummy_file, 'w') as write_obj:
        for line in list_of_lines:
            write_obj.write(line + '\n')
        for line in read_obj:
            write_obj.write(line)
    os.remove(file_name)
    os.rename(dummy_file, file_name)

prepend_multiple_lines("./data/sample.txt", list_of_lines)