###############################################################################
### return 2 numbers from the list only  if they total a given target s number
##############################################################################
ar = [3, 5, -4, 8, 11, 1, -1, 6]
s = 10

res1 = [t for x in set(ar) if (t:=(s - x)) in set(ar) and t is not x]

def twonumberSum(array, targetSum):
  storage = set(num for num in array)
  for num in array:
      target = targetSum - num
      if target in storage and target is not num:
        return [num, target]

res2 = twonumberSum(ar,10)

print(res1)
print(res2)
