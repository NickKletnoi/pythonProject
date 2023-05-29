#################################################################################
#### Determine the sequece of the first set if it is present in the second set
#################################################################################
array = [5,1,22,25,6,-1,8,10]
sequence = [1,6,3]

def isValidSubsequence(array, sequence):
    storage = []
    isvalid = 0
    for item in array:
        if item in sequence:
            storage.append(item)
    if len(storage) == len(sequence):
        isvalid = 1
    return isvalid

v = isValidSubsequence(array,sequence)
print(v)
##########################################
