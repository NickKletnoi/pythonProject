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
import os, uuid, sys
from azure.storage.blob import BlobClient, BlobServiceClient, ContainerClient, PublicAccess, __version__
from azure.storage.blob.blockblobservice import BlockBlobService
import re

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

processing_list =[]

formUrl_part_1 = "https://ecgsdatastore.blob.core.windows.net/cograw/"
formUrl_part_2 = "?sp=r&st=2023-04-28T14:20:41Z&se=2024-03-30T22:20:41Z&spr=https&sv=2021-12-02&sr=b&sig=bl%2BQh%2F2zveJCS3Xu4MQobPk0v0MPyPuS%2FmANAA1JE%2Fw%3D"

conn_str = "DefaultEndpointsProtocol=https;AccountName=ecgsdatastore;AccountKey=MRyWERByKo6Q8FWoOxSaqwgHgQjxoiBhlODkkIQ05J/6ciDVNBqDwJLgijvRou83V3FALN67/YIBToZHAlL5SQ==;EndpointSuffix=core.windows.net"
blob_service_client = BlobServiceClient.from_connection_string(conn_str)
blob_list = blob_service_client.get_container_client('cograw').list_blobs()
final_processing_list = [formUrl_part_1 +
                      str(i.name) + formUrl_part_2 for i in blob_list]

for blob in final_processing_list:
    print(blob)

# for blob in blob_list:
#     print(blob)





############################################################################
# blob_service = BlobServiceClient.from_connection_string(conn_str)
#
# blobs = []
# marker = None
# while True:
#     batch = blob_service.list_blobs('cograw', marker=marker)
#     blobs.extend(batch)
#     if not batch.next_marker:
#         break
#     marker = batch.next_marker
# for blob in blobs:
#     print(blob.name)



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