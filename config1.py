import pyodbc
import urllib
from sqlalchemy import create_engine

DATABASE_CONFIG = {
        'server': 'tcp:myserver.database.windows.net',
        'user': 'dbuser',
        'password': 'password',
        'dbname': 'dbname',
        'conn_str': 'DSN=DEV2;autocommit=True',
        'conn_str2': 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=interlake-bi.database.windows.net;DATABASE=ISS_DW;UID=BIAdmin;PWD=sb98D&B(*#$@'

    }

#https://www.alirookie.com/post/azure-functions-with-python-first-steps-towards-clean-code
class con(object):
    def __init__(self):
        self.engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % urllib.parse.quote_plus(DATABASE_CONFIG['conn_str2']))
    @property
    def cn(self): return self.cn
    @cn.setter
    def cn(self, value): self.cn = value
    @cn.deleter
    def cn(self): self.cn = None


# engine.cn = 10
# print(engine.cn)
# del engine.cn

tbl1 = dict(
    source1='target1',
    source2='target2',
)

tbl2 = dict(
    source3='target3',
    source4='target4',
    exceptions=dict(
        special_case1='Special case value 1',
        special_case2='Special case value 2',
    ),
    pv=2
)


workspaceEnvironmentDict = {
  "lifecycle": "Development",
  "adlsGen1": "comcastdevtest",
  "adlsGen2StorageAccount": "cutdpbxdtlk01",
  "adlsGen2Container": "comcastdev",
  "edwDataLakeKeyVaultScope": {"firstscope":"cuttkyvtdtlkedw01",
                                "secondscope":"cuttkyvtdtlkedw02"
                                }
}

basePaths = {"adlsGen1BasePath": "/mnt/gen1/" + workspaceEnvironmentDict["adlsGen1"],
             "adlsGen2BasePath": "/mnt/" + workspaceEnvironmentDict["adlsGen2StorageAccount"] + "/" + workspaceEnvironmentDict["adlsGen2Container"],
            }