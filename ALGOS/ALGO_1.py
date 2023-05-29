
################################################################################
#### return 2 numbers from the list only  if they total a given target s number
ar = [3, 5, -4, 8, 11, 1, -1, 6]
s = 10

def twonumberSum(array, targetSum):
  storage = set(num for num in array)

  for num in array:
      target = targetSum - num
      if target in storage and target is not num:
        return [num, target]

res = twonumberSum(ar,10)

print(res)
#################################################################################