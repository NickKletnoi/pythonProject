import pyodbc

DATABASE_CONFIG = {
        'server': 'tcp:myserver.database.windows.net',
        'user': 'dbuser',
        'password': 'password',
        'dbname': 'dbname',
        'conn_str': 'DSN=DEV2;autocommit=True'

    }

class Connection:
    def __init__(self):
        self.db=pyodbc.connect(DATABASE_CONFIG['conn_str'])
        self.cur = self.db.cursor()



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