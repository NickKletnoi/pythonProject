import pyodbc
import pandas as pd

#test1.cabh7z7j0wfy.us-east-1.rds.amazonaws.com
#admin
#RQxhnpY51mnIKqFtYHlZ

server = 'tcp:test1.cabh7z7j0wfy.us-east-1.rds.amazonaws.com'
database = 'test1'
username = 'admin'
password = 'RQxhnpY51mnIKqFtYHlZ'
cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)

cursor = cnxn.cursor()

#Sample select query
query = 'SELECT....'
df = pd.sql_read(query, cnxn)
print(df)