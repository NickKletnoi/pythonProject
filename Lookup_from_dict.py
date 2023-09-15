import pandas as pd

df = pd.DataFrame()
df['letter'] = ['a', 'a', 'c', 'd', 'd','h']
lkup = {'a':'b', 'b':'c', 'c':'d', 'd':'e', 'e':'f'}
df['newletter'] = df['letter'].map(lkup)
df["newletter"].fillna("NA", inplace = True)
print(df)

