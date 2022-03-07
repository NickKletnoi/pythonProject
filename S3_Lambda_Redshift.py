# from datetime import datetime, timedelta
# import dateutil.tz
# import psycopg2
# from config import *
#
#
# def lambda_handler(event, context):
#     con = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
#     cur = con.cursor()
#
#     try:
#         query = """BEGIN TRANSACTION;
#
#                 COPY """ + table_name + """ FROM '""" + intermediate_path + """' iam_role '""" + iam_role + """' FORMAT AS parquet;
#
#                 END TRANSACTION;"""
#
#         print(query)
#         cur.execute(query)
#
#     except Exception as e:
#         subject = "Error emr copy: {}".format(str(datetime.now().date()))
#         body = "Exception occured " + str(e)
#         print(body)
#
#     con.close()