from azure.core.credentials import AzureKeyCredential
from azure.ai.formrecognizer import DocumentAnalysisClient
import pandas as pd
import datetime
import urllib
from sqlalchemy import create_engine
start_time = datetime.datetime.now()
current_time = start_time.strftime("%Y-%m-%d")
dfcols = ["ExtractDate", "ReportName", "KeyName","ValueName"]
rows = []

con_str = urllib.parse.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};SERVER=interlake-bi.database.windows.net;DATABASE=ISS_DW;UID=BIAdmin;PWD=sb98D&B(*#$@")
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % con_str)

endpoint = "https://testpdfrecognize.cognitiveservices.azure.com/"
key = "f251c939777240a092289a7c5d2ac60d"

model_id = "US_Market_1st_5_metrics"
formUrl = "https://ecgsdatastore.blob.core.windows.net/cograw/US_20230215_EXAMPLE.pdf?sp=r&st=2023-04-28T14:20:41Z&se=2024-03-30T22:20:41Z&spr=https&sv=2021-12-02&sr=b&sig=bl%2BQh%2F2zveJCS3Xu4MQobPk0v0MPyPuS%2FmANAA1JE%2Fw%3D"

document_analysis_client = DocumentAnalysisClient(
    endpoint=endpoint, credential=AzureKeyCredential(key)
)

key_list = ['West Texas Int','NYMEX Crude','Mars','WTI Posting Plus']

poller = document_analysis_client.begin_analyze_document_from_url(model_id, formUrl)
result = poller.result()

for idx, document in enumerate(result.documents):
    if idx == 0:
        for name, field in document.fields.items():
            field_value = field.value if field.value else field.content
            field_name = name
            if field_name in key_list and field_name == 'WTI Posting Plus':
                 wti_posting_plus = field_value
                 rows.append({"ExtractDate": current_time, "ReportName": "US Marketscan 02-15-2023", "KeyName": "WTI Posting Plus", "ValueName": wti_posting_plus})
            if field_name in key_list and field_name == 'NYMEX Crude':
                 nymex_crude = field_value
                 rows.append({"ExtractDate": current_time, "ReportName": "US Marketscan 02-15-2023", "KeyName": "NYMEX Crude", "ValueName": nymex_crude})
            if field_name in key_list and field_name == 'Mars':
                 mars = field_value
                 rows.append({"ExtractDate": current_time, "ReportName": "US Marketscan 02-15-2023", "KeyName": "Mars", "ValueName": mars})
            if field_name in key_list and field_name == 'West Texas Int':
                 west_texas_int = field_value
                 rows.append({"ExtractDate": current_time, "ReportName": "US Marketscan 02-15-2023", "KeyName": "West Texas Int", "ValueName": west_texas_int})


df = pd.DataFrame(rows, columns=dfcols)
df.to_sql('PDFExtract', con=engine, index=False, if_exists='append', schema='dbo')


