import boto3
import Include
import os
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
local_upload_dir = Include.setDir()
local_upload_dir2 = Include.setDir2()
conn_str = Include.setAzureBlobConn()
container_name = Include.setAzureContainer()
blob_service_client = BlobServiceClient.from_connection_string(conn_str=conn_str)
######################################
aws_target_path = Include.setAWSTargetPath()
aws_access_keyID = Include.setAWSAccessKeyID()
aws_secret_access_key = Include.setAWSecretAccessKey()
aws_region_name = Include.setAWSRegionName()
aws_s3 = Include.setAWSResourceS3()
aws_bucket = Include.setAWSBucket()

session = boto3.Session(
    aws_access_key_id= aws_access_keyID,
    aws_secret_access_key= aws_secret_access_key,
    region_name= aws_region_name
)
s3 = session.resource(aws_s3)
bucket = s3.Bucket(aws_bucket)

for file_name in os.listdir(local_upload_dir):
    blob_client = blob_service_client.get_blob_client(container=container_name,blob=file_name)
    with open(local_upload_dir2+file_name,"rb") as data:
        complete_file_path = local_upload_dir2+file_name
        file_basename = os.path.basename(complete_file_path)
        final_filename = aws_target_path + file_basename
        bucket.put_object(Key=final_filename, Body=data)

print("**Completed**")







