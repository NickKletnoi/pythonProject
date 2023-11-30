import pandas as pd

revenue_wide = pd.read_csv('https://learn.sharpsightlabs.com/datasets/pdm/revenue_wide.csv')

rw = revenue_wide.melt(id_vars = ['region']
                  ,value_vars = ['Q1','Q2']
                  ,value_name = 'revenue'
                  ,var_name = 'quarter'
                  )

print(rw)