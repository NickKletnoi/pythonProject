import Include
import pyodbc
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import create_engine
import subprocess
import glob
import shutil
import os
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

connStringRed = create_engine(Include.setConnRed())
conString = Include.setConn()
fileString = Include.setFile()
sourceDir = Include.setSouceDir()
sqlTblString = Include.setSQLTbls()
fileTail = Include.setFileTail()
conn = pyodbc.connect(conString)
sqlTb = pd.read_sql(sqlTblString, conn)
dfTbl = pd.DataFrame(data=sqlTb)
for i, row in dfTbl.iterrows():
    sql2 = pd.read_sql(row["ExtractionSQL"], conn)
    df9 = pd.DataFrame(data=sql2)
    parquet_file = row["Location"] + row["TgtSchName"] + "_" + row["TblName"] + fileTail
    parquet_file = sourceDir + parquet_file
    parquet_schema = pa.Table.from_pandas(df9).schema
    parquet_writer = pq.ParquetWriter(parquet_file, parquet_schema, compression='snappy')
    df9.to_sql('orders', connStringRed, index=False, if_exists='replace',schema='rb')
    table = pa.Table.from_pandas(df9, parquet_schema)
    parquet_writer.write_table(table)
    print(parquet_file)
    parquet_writer.close()

conn.close()

print("** Parquet file and Load to Redshift Completed**")

dest_dir = Include.setTargetDir()
source_dir = Include.setSouceDir()
match_cond = Include.setMatchCond()
for file in glob.glob(source_dir + match_cond):
    print(file)
    shutil.copy(file, dest_dir)

print("** Move to Upload Directory Completed**")

local_upload_dir = Include.setDir()
local_upload_dir2 = Include.setDir2()
conn_str = Include.setAzureBlobConn()
container_name = Include.setAzureContainer()
blob_service_client = BlobServiceClient.from_connection_string(conn_str=conn_str)

for file_name in os.listdir(local_upload_dir):

    blob_client = blob_service_client.get_blob_client(container=container_name,blob=file_name)
    with open(local_upload_dir2+file_name,"rb") as data:
        blob_client.upload_blob(data,overwrite=True)

print("** Upload to Azure Storage Completed**")

result2 = subprocess.Popen("UploadToS3.py", shell=True)

print("** Upload to S3 Storage Completed**")














