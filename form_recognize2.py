from azure.core.credentials import AzureKeyCredential
from azure.ai.formrecognizer import DocumentAnalysisClient
from sqlalchemy import create_engine
import pandas as pd
import datetime
import urllib
start_time = datetime.datetime.now()
current_time = start_time.strftime("%Y-%m-%d")
dfcols = ["ExtractDate", "ReportName", "KeyName","ValueName"]
rows = []

con_str = urllib.parse.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};SERVER=interlake-bi.database.windows.net;DATABASE=ISS_DW;UID=BIAdmin;PWD=sb98D&B(*#$@")
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % con_str)
endpoint = "https://testpdfrecognize.cognitiveservices.azure.com/"
key = "f251c939777240a092289a7c5d2ac60d"
formUrl = "https://ecgsdatastore.blob.core.windows.net/cograw/US_20230215_EXAMPLE.pdf?sp=r&st=2023-04-28T14:20:41Z&se=2024-03-30T22:20:41Z&spr=https&sv=2021-12-02&sr=b&sig=bl%2BQh%2F2zveJCS3Xu4MQobPk0v0MPyPuS%2FmANAA1JE%2Fw%3D"

document_analysis_client = DocumentAnalysisClient(
    endpoint=endpoint, credential=AzureKeyCredential(key)
)

poller = document_analysis_client.begin_analyze_document_from_url("prebuilt-document", formUrl)
result = poller.result()

key_list = ['West Texas Int','NYMEX Crude','Mars','WTI Posting Plus','Group 3','LA Pipeline','SF Pipeline']

for kv_pair in result.key_value_pairs:
    if kv_pair.key.content in key_list and kv_pair.key.content == 'West Texas Int':
        west_texas_int = kv_pair.value.content
        rows.append({"ExtractDate": current_time, "ReportName": "US Marketscan 02-15-2023", "KeyName": "West Texas Int", "ValueName":west_texas_int })

    if kv_pair.key.content in key_list and kv_pair.key.content == 'NYMEX Crude':
        nymex_crude = kv_pair.value.content
        rows.append({"ExtractDate": current_time, "ReportName": "US Marketscan 02-15-2023", "KeyName": "NYMEX Crude","ValueName": nymex_crude})

    if kv_pair.key.content in key_list and kv_pair.key.content == 'Mars':
        mars = kv_pair.value.content
        rows.append({"ExtractDate": current_time, "ReportName": "US Marketscan 02-15-2023", "KeyName": "Mars","ValueName": mars})

    if kv_pair.key.content in key_list and kv_pair.key.content == 'WTI Posting Plus':
        wti_posting_plus = kv_pair.value.content
        rows.append({"ExtractDate": current_time, "ReportName": "US Marketscan 02-15-2023", "KeyName": "WTI Posting Plus", "ValueName": wti_posting_plus})

    if kv_pair.key.content in key_list and kv_pair.key.content == 'Group 3':
        group_3 = kv_pair.value.content
        rows.append({"ExtractDate": current_time, "ReportName": "US Marketscan 02-15-2023", "KeyName": "Group 3","ValueName": group_3})

    if kv_pair.key.content in key_list and kv_pair.key.content == 'LA Pipeline':
        la_Pipeline = kv_pair.value.content
        rows.append({"ExtractDate": current_time, "ReportName": "US Marketscan 02-15-2023", "KeyName": "LA Pipeline", "ValueName": la_Pipeline})

    if kv_pair.key.content in key_list and kv_pair.key.content == 'SF Pipeline':
       sf_Pipeline = kv_pair.value.content
       rows.append({"ExtractDate": current_time, "ReportName": "US Marketscan 02-15-2023", "KeyName": "SF Pipeline","ValueName": sf_Pipeline})

df = pd.DataFrame(rows, columns=dfcols)
df.to_sql('PDFExtract', con=engine, index=False, if_exists='append', schema='dbo')

