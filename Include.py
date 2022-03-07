import Include
from datetime import datetime
import os
from azure.storage.blob import BlobServiceClient

class DirectoryClient:
  def __init__(self, connection_string, container_name):
    service_client = BlobServiceClient.from_connection_string(connection_string)
    self.client = service_client.get_container_client(container_name)

  def upload(self, source, dest):
    '''
    Upload a file or directory to a path inside the container
    '''
    if (os.path.isdir(source)):
      self.upload_dir(source, dest)
    else:
      self.upload_file(source, dest)

  def upload_file(self, source, dest):
    '''
    Upload a single file to a path inside the container
    '''
    print(f'Uploading {source} to {dest}')
    with open(source, 'rb') as data:
      self.client.upload_blob(name=dest, data=data)

  def upload_dir(self, source, dest):
    '''
    Upload a directory to a path inside the container
    '''
    prefix = '' if dest == '' else dest + '/'
    prefix += os.path.basename(source) + '/'
    for root, dirs, files in os.walk(source):
      for name in files:
        dir_part = os.path.relpath(root, source)
        dir_part = '' if dir_part == '.' else dir_part + '/'
        file_path = os.path.join(root, name)
        blob_path = prefix + dir_part + name
        self.upload_file(file_path, blob_path)

  def download(self, source, dest):
    '''
    Download a file or directory to a path on the local filesystem
    '''
    if not dest:
      raise Exception('A destination must be provided')

    blobs = self.ls_files(source, recursive=True)
    if blobs:
      # if source is a directory, dest must also be a directory
      if not source == '' and not source.endswith('/'):
        source += '/'
      if not dest.endswith('/'):
        dest += '/'
      # append the directory name from source to the destination
      dest += os.path.basename(os.path.normpath(source)) + '/'

      blobs = [source + blob for blob in blobs]
      for blob in blobs:
        blob_dest = dest + os.path.relpath(blob, source)
        self.download_file(blob, blob_dest)
    else:
      self.download_file(source, dest)

  def download_file(self, source, dest):
    '''
    Download a single file to a path on the local filesystem
    '''
    # dest is a directory if ending with '/' or '.', otherwise it's a file
    if dest.endswith('.'):
      dest += '/'
    blob_dest = dest + os.path.basename(source) if dest.endswith('/') else dest

    print(f'Downloading {source} to {blob_dest}')
    os.makedirs(os.path.dirname(blob_dest), exist_ok=True)
    bc = self.client.get_blob_client(blob=source)
    with open(blob_dest, 'wb') as file:
      data = bc.download_blob()
      file.write(data.readall())

  def ls_files(self, path, recursive=False):
    '''
    List files under a path, optionally recursively
    '''
    if not path == '' and not path.endswith('/'):
      path += '/'

    blob_iter = self.client.list_blobs(name_starts_with=path)
    files = []
    for blob in blob_iter:
      relative_path = os.path.relpath(blob.name, path)
      if recursive or not '/' in relative_path:
        files.append(relative_path)
    return files

  def ls_dirs(self, path, recursive=False):
    '''
    List directories under a path, optionally recursively
    '''
    if not path == '' and not path.endswith('/'):
      path += '/'

    blob_iter = self.client.list_blobs(name_starts_with=path)
    dirs = []
    for blob in blob_iter:
      relative_dir = os.path.dirname(os.path.relpath(blob.name, path))
      if relative_dir and (recursive or not '/' in relative_dir) and not relative_dir in dirs:
        dirs.append(relative_dir)

    return dirs

  def rm(self, path, recursive=False):
    '''
    Remove a single file, or remove a path recursively
    '''
    if recursive:
      self.rmdir(path)
    else:
      print(f'Deleting {path}')
      self.client.delete_blob(path)

  def rmdir(self, path):
    '''
    Remove a directory and its contents recursively
    '''
    blobs = self.ls_files(path, recursive=True)
    if not blobs:
      return

    if not path == '' and not path.endswith('/'):
      path += '/'
    blobs = [path + blob for blob in blobs]
    print(f'Deleting {", ".join(blobs)}')
    self.client.delete_blobs(*blobs)

######################### FUNCTIONS ############################################



#################################################################################
def setConn():
    conString = 'DSN=DEV;autocommit=True'
    return conString
def setConnRed2():
    conStringRed2 = 'DSN=Redshift_RB;autocommit=True'
    return conStringRed2
def setConnRed():
    conStringRed = 'postgresql://reservebar-master:0$sd^e2ivN!9xP!MO4Mr@reservebar-master.cosesp8bmzst.us-west-2.redshift.amazonaws.com:5439/reservebar-master'
    return conStringRed
def setDir():
    local_dir = 'C:/temp/Orders/'
    return local_dir
def setDir2():
    local_dir2 = "C:\\temp\\Orders\\"
    return local_dir2
def setAzureBlobConn():
    blob_conn_str = "DefaultEndpointsProtocol=https;AccountName=nikblobstorage;AccountKey=Eb7XVUBvtKrwAf57yedqGeFub5IBAGeuyh3lPwY141sVsM9wg96Mm/p8wusKJjzwoEe/e+iXwW7/2ZG4PCo5OQ==;EndpointSuffix=core.windows.net"
    return blob_conn_str
def setAzureContainer():
    container_name = "raw/Orders"
    return container_name
def setSQLTbls():
    sqlTblString = "select * from [codegen].[TblSynMeta] where [ExportFlg]='Y'"
    return sqlTblString
def setSQLTblsHwm():
    sqlTblStringHwm = "select top 1 watermark_value from rb.etl_watermark order by watermark_value desc"
    return sqlTblStringHwm
def setFile():
    fileString = 'data\EngagementProfile11.parquet'
    return fileString
def setFileTail():
    dateTimeObj = datetime.now()
    timestampStr = dateTimeObj.strftime("%m_%d_%Y_%H_%M_%S_%f")
    fileTail = "_" + timestampStr + "_"
    return fileTail
def setSouceDir():
    source_dir = 'C:/Users/P5104926/PycharmProjects/pythonProject/data/Orders/'
    return source_dir
def setTargetDir():
    target_dir = 'C:/temp/Orders/'
    return target_dir
def setAzureContainer2():
    container_name2 = "raw"
    return container_name2
def setMatchCond():
    match_cond = '*.parquet'
    return match_cond
def setMatchCondOrd():
    match_cond_ord = 'stg_order*.*'
    return match_cond_ord
def setDbfile():
    db_file = "C:\sqlite\chinook.db"
    return db_file
def setAPIKey():
    api_key = 'abefe4590454c0ecde06b0b1b0761b99'
    return api_key
def setAPIAgent():
    user_agent = 'MyAgent'
    return user_agent
def setAPIURL():
    api_url = "https://commerceapi.io/api/v1/Orders?Filters.status=150&Filters.from=2020-12-20&Filters.senderCompanyId=127409&subscription-key=528D61CA-E652-465F-8310-1645490A0857"
    return api_url
def setAPIURLNew():
    api_url_new = "https://commerceapi.io/api/v1/Orders"
    return api_url_new
def setAPIHost():
    api_host = "LogicBroker.api.v01.com"
    return api_host
def setAPIContentType():
    api_contentype = "application/json"
    return api_contentype
################################################
def setAWSTargetPath ():
    aws_targetpath = 'stg_order/'
    return aws_targetpath
def setAWSAccessKeyID ():
    aws_accesskeyid = 'AKIA23ZIY2CNDYQKJJV5'
    return aws_accesskeyid
def setAWSecretAccessKey ():
    aws_secretaccesskey = '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'
    return aws_secretaccesskey
def setAWSRegionName():
    aws_regionname = 'us-west-2'
    return aws_regionname
def setAWSResourceS3():
    aws_resources3 = 's3'
    return aws_resources3
def setAWSBucket():
    aws_bucketname = 'reservebar'
    return aws_bucketname
###############################################
def setAPIsubscriptionkey():
    api_subscriptionkey = '528D61CA-E652-465F-8310-1645490A0857'
    return api_subscriptionkey
def setAPIFiltersSenderCompanyID():
    api_sendercompanyid = '127409'
    return api_sendercompanyid
def setAPIFilterfrom():
    api_filterfrom = '2020-12-22'
    return api_filterfrom
def setAPIFilterstatus():
    api_filterstatus = '150'
    return api_filterstatus
###########################################################
def setAPIURLNewV2():
    api_url_new_v2 = "https://commerceapi.io/api/v2/Orders"
    return api_url_new_v2
def setAPIFilterfromNew():
    api_filterfrom_new = '2020-12-01T00:00'
    return api_filterfrom_new
def setAPIFilterToNew():
    api_filterTo_new = '2020-12-02T00:00'
    return api_filterTo_new

def setAPIPayLoad():
    payload = {
        "subscription-key": Include.setAPIsubscriptionkey(),
        "Filters.senderCompanyId": Include.setAPIFiltersSenderCompanyID(),
        "Filters.from": Include.setAPIFilterfromNew(),
        "Filters.to": Include.setAPIFilterToNew(),
        "Filters.status": Include.setAPIFilterstatus(),
        "Filters.pageSize": 1
    }
    return payload
def setReserveBarDir():
    rb_dir = 'C:\\Users\\P5104926\\OneDrive\\Reservebar\\Data\\'
    return rb_dir
def setReserveBarDirLoc():
    rb_dir_loc = 'C:\\Users\\P5104926\\PycharmProjects\\pythonProject\\data\\JSON\\'
    return rb_dir_loc
def setFileTailRb():
    dateTimeObj = datetime.now()
    timestampStr = dateTimeObj.strftime("%m_%d_%Y_%H_%M_%S_%f")
    fileTailRb = "_" + timestampStr + ".parquet.gzip"
    return fileTailRb
def setFileTailRbJ():
    dateTimeObj = datetime.now()
    timestampStr = dateTimeObj.strftime("%m_%d_%Y_%H_%M_%S_%f")
    fileTailRbJ = "_" + timestampStr + ".json"
    return fileTailRbJ
def setFileTailRbCsv():
    dateTimeObj = datetime.now()
    timestampStr = dateTimeObj.strftime("%m_%d_%Y_%H_%M_%S_%f")
    fileTailRbCsv = "_" + timestampStr + ".csv"
    return fileTailRbCsv
def setS3UploadFileOrders():
    s3_upload_ord = 'UploadToS3.py'
    return s3_upload_ord



