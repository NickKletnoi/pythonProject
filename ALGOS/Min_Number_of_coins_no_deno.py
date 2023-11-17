
#return the min sum you cannot create given the sequence

coins = [5,7,1,1,2,3,22]

def nonConstructibleChange(coins):
  coins.sort()
  minimum_change = 0
  for coin in coins:
    if coin > minimum_change + 1:
      break
    minimum_change += coin
  return minimum_change + 1

print(nonConstructibleChange(coins))
