from azure.core.credentials import AzureKeyCredential
from azure.ai.formrecognizer import DocumentAnalysisClient
import pandas as pd
import datetime
import urllib
from sqlalchemy import create_engine
start_time = datetime.datetime.now()
current_time = start_time.strftime("%Y-%m-%d")
from datetime import datetime, timedelta
from azure.storage.blob import BlobClient, generate_blob_sas, BlobSasPermissions
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

processing_list =[]

included_extensions = ['pdf']
dfcols = ["ExtractDate", "ReportName", "KeyName","ValueName"]
rows = []
con_str = urllib.parse.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};SERVER=interlake-bi.database.windows.net;DATABASE=ISS_DW;UID=BIAdmin;PWD=sb98D&B(*#$@")
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % con_str)
endpoint = "https://testpdfrecognize.cognitiveservices.azure.com/"
key = "f251c939777240a092289a7c5d2ac60d"
model_id = "US_Market_Scan_Extraction_Model_20"
formUrl = "https://ecgsdatastore.blob.core.windows.net/cograw/US_20230215_EXAMPLE.pdf?sp=r&st=2023-04-28T14:20:41Z&se=2024-03-30T22:20:41Z&spr=https&sv=2021-12-02&sr=b&sig=bl%2BQh%2F2zveJCS3Xu4MQobPk0v0MPyPuS%2FmANAA1JE%2Fw%3D"

account_name = 'ecgsdatastore'
account_key = 'MRyWERByKo6Q8FWoOxSaqwgHgQjxoiBhlODkkIQ05J/6ciDVNBqDwJLgijvRou83V3FALN67/YIBToZHAlL5SQ=='
container_name = 'cograw'
ReportName = "US Marketscan"

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
blob_list = blob_service_client.get_container_client(container_name).list_blobs()

filtered_blob_list = [str(fn.name) for fn in blob_list if any(str(fn.name).endswith(ext) for ext in included_extensions)]
final_processing_list = [formUrl_part_1 + str(filename) + '?' + get_blob_sas(account_name,account_key, container_name, filename) for filename in filtered_blob_list]

def process_form(formUrl):

    document_analysis_client = DocumentAnalysisClient(
        endpoint=endpoint, credential=AzureKeyCredential(key)
    )

    key_list = [
        'AATGX00',
        'POAEG00',
        'PUAAO00',
        'PUAAX00',
        'ReportDate',
        'AATGY00',
        'POAED00',
        'AATGZ00',
        'POAEE00',
        'PUAAI00',
        'PUBDM00',
        'AATHA00',
        'AATHB00',
        'AUGMA00',
        'AUAMA00',
        'ICIC001',
        'NMNG001',
        'NMHO001',
        'DELU001',
        'DETR001'
    ]

    #ReportName = "US Marketscan"

    poller = document_analysis_client.begin_analyze_document_from_url(model_id, formUrl)
    result = poller.result()

    for idx, document in enumerate(result.documents):
            for name, field in document.fields.items():
                field_value = field.value if field.value else field.content
                field_name = name
                if field_name in key_list and field_name == 'AATGX00':
                     AATGX00 = field_value
                     rows.append({"ExtractDate": current_time, "ReportName": ReportName, "KeyName": "AATGX00", "ValueName": AATGX00})

                if field_name in key_list and field_name == 'POAEG00':
                     POAEG00 = field_value
                     rows.append({"ExtractDate": current_time, "ReportName": ReportName, "KeyName": "POAEG00", "ValueName": POAEG00})

                if field_name in key_list and field_name == 'PUAAO00':
                     PUAAO00 = field_value
                     rows.append({"ExtractDate": current_time, "ReportName": ReportName, "KeyName": "PUAAO00", "ValueName": PUAAO00})

                if field_name in key_list and field_name == 'PUAAX00':
                     PUAAX00 = field_value
                     rows.append({"ExtractDate": current_time, "ReportName": ReportName, "KeyName": "PUAAX00", "ValueName": PUAAX00})

                if field_name in key_list and field_name == 'ReportDate':
                     ReportDate = field_value
                     rows.append({"ExtractDate": current_time, "ReportName": ReportName, "KeyName": "RPTDATE", "ValueName": ReportDate})

                if field_name in key_list and field_name == 'AATGY00':
                     AATGY00 = field_value
                     rows.append({"ExtractDate": current_time, "ReportName": ReportName, "KeyName": "AATGY00", "ValueName": AATGY00})

                if field_name in key_list and field_name == 'POAED00':
                     POAED00 = field_value
                     rows.append({"ExtractDate": current_time, "ReportName": ReportName, "KeyName": "POAED00", "ValueName": POAED00})

                if field_name in key_list and field_name == 'AATGZ00':
                     AATGZ00 = field_value
                     rows.append({"ExtractDate": current_time, "ReportName": ReportName, "KeyName": "AATGZ00", "ValueName": AATGZ00})

                if field_name in key_list and field_name == 'POAEE00':
                     POAEE00 = field_value
                     rows.append({"ExtractDate": current_time, "ReportName": ReportName, "KeyName": "POAEE00", "ValueName": POAEE00})

                if field_name in key_list and field_name == 'PUAAI00':
                     PUAAI00 = field_value
                     rows.append({"ExtractDate": current_time, "ReportName": ReportName, "KeyName": "PUAAI00", "ValueName": PUAAI00})

                if field_name in key_list and field_name == 'PUBDM00':
                     PUBDM00 = field_value
                     rows.append({"ExtractDate": current_time, "ReportName": ReportName, "KeyName": "PUBDM00", "ValueName": PUBDM00})

                if field_name in key_list and field_name == 'AATHA00':
                     AATHA00 = field_value
                     rows.append({"ExtractDate": current_time, "ReportName": ReportName, "KeyName": "AATHA00", "ValueName": AATHA00})

                if field_name in key_list and field_name == 'AATHB00':
                     AATHB00 = field_value
                     rows.append({"ExtractDate": current_time, "ReportName": ReportName, "KeyName": "AATHB00", "ValueName": AATHB00})

                if field_name in key_list and field_name == 'AUGMA00':
                     AUGMA00 = field_value
                     rows.append({"ExtractDate": current_time, "ReportName": ReportName, "KeyName": "AUGMA00", "ValueName": AUGMA00})

                if field_name in key_list and field_name == 'AUAMA00':
                     AUAMA00 = field_value
                     rows.append({"ExtractDate": current_time, "ReportName": ReportName, "KeyName": "AUAMA00", "ValueName": AUAMA00})

                if field_name in key_list and field_name == 'ICIC001':
                     ICIC001 = field_value
                     rows.append({"ExtractDate": current_time, "ReportName": ReportName, "KeyName": "ICIC001", "ValueName": ICIC001})

                if field_name in key_list and field_name == 'NMNG001':
                     NMNG001 = field_value
                     rows.append({"ExtractDate": current_time, "ReportName": ReportName, "KeyName": "NMNG001", "ValueName": NMNG001})

                if field_name in key_list and field_name == 'NMHO001':
                     NMHO001 = field_value
                     rows.append({"ExtractDate": current_time, "ReportName": ReportName, "KeyName": "NMHO001", "ValueName": NMHO001})

                if field_name in key_list and field_name == 'DELU001':
                     DELU001 = field_value
                     rows.append({"ExtractDate": current_time, "ReportName": ReportName, "KeyName": "DELU001", "ValueName": DELU001})

                if field_name in key_list and field_name == 'DETR001':
                     DETR001 = field_value
                     rows.append({"ExtractDate": current_time, "ReportName": ReportName, "KeyName": "DETR001", "ValueName": DETR001})


    df = pd.DataFrame(rows, columns=dfcols)
    #df['ReportDate'] = ReportDate
    #final_df = df.astype({'ReportDate': 'string'})
    df.to_sql('PDFExtract', con=engine, index=False, if_exists='append', schema='dbo')

print(final_processing_list)

process_docs = [process_form(form_url) for form_url in final_processing_list]

