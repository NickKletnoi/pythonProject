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
conn_str = "DefaultEndpointsProtocol=https;AccountName=ecgsdatastore;AccountKey=MRyWERByKo6Q8FWoOxSaqwgHgQjxoiBhlODkkIQ05J/6ciDVNBqDwJLgijvRou83V3FALN67/YIBToZHAlL5SQ==;EndpointSuffix=core.windows.net"
from azure.storage.blob import *

blob_service = BlobServiceClient.from_connection_string(conn_str)

blobs = []
marker = None
while True:
    batch = blob_service.list_blobs('rawdata', marker=marker)
    blobs.extend(batch)
    if not batch.next_marker:
        break
    marker = batch.next_marker
for blob in blobs:
    print(blob.name)



    ###################################

# local_upload_dir = Include.setDir()
# local_upload_dir2 = Include.setDir2()
# container_name = Include.setAzureContainer()
# blob_service_client = BlobServiceClient.from_connection_string(conn_str)
#
# for file_name in os.listdir(local_upload_dir):
#
#     blob_client = blob_service_client.get_blob_client(container=container_name,blob=file_name)
#     with open(local_upload_dir2+file_name,"rb") as data:
#         blob_client.upload_blob(data,overwrite=True)
#
# print("** Upload to Azure Storage Completed**")