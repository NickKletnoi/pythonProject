from datetime import datetime, timedelta
from azure.storage.blob import BlobClient, generate_blob_sas, BlobSasPermissions
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

processing_list =[]

account_name = 'ecgsdatastore'
account_key = 'MRyWERByKo6Q8FWoOxSaqwgHgQjxoiBhlODkkIQ05J/6ciDVNBqDwJLgijvRou83V3FALN67/YIBToZHAlL5SQ=='
container_name = 'cograw'

formUrl_part_1 = "https://ecgsdatastore.blob.core.windows.net/cograw/"
conn_str = "DefaultEndpointsProtocol=https;AccountName=ecgsdatastore;AccountKey=MRyWERByKo6Q8FWoOxSaqwgHgQjxoiBhlODkkIQ05J/6ciDVNBqDwJLgijvRou83V3FALN67/YIBToZHAlL5SQ==;EndpointSuffix=core.windows.net"

def get_blob_sas(account_name,account_key, container_name, blob_name):
    sas_blob = generate_blob_sas(account_name=account_name,
    container_name=container_name,
    blob_name=blob_name,
    account_key=account_key,
    permission=BlobSasPermissions(read=True),
    expiry=datetime.utcnow() + timedelta(hours=1))
    return sas_blob

blob_service_client = BlobServiceClient.from_connection_string(conn_str)
blob_list = blob_service_client.get_container_client('cograw').list_blobs()

included_extensions = ['pdf']
file_names = [str(fn.name) for fn in blob_list if any(str(fn.name).endswith(ext) for ext in included_extensions)]

print(file_names)


