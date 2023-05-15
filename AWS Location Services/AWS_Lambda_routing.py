import json
import boto3
import botocore
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

location = boto3.client("location", config=botocore.config.Config(user_agent="Amazon Redshift"))
CALCULATOR_NAME = 'MyRouteCalculator'


def calculate_route(dep_lon, dep_lat, dest_lon, dest_lat):
    response = location.calculate_route_matrix(
        CalculatorName=CALCULATOR_NAME,
        DeparturePositions=[[dep_lon, dep_lat]],
        DestinationPositions=[[dest_lon, dest_lat]])

    if len(response) >= 1:
        distance = response["RouteMatrix"][0][0]["Distance"]
        duration = response["RouteMatrix"][0][0]["DurationSeconds"]

        res = {
            "Distance": distance,
            "Duration": duration
        }

    return res


def lambda_handler(event, context):
    try:
        response = dict()
        records = event["arguments"]
        results = []

        for record in records:
            dep_lon_f, dep_lat_f, dest_lon_f, dest_lat_f = record
            try:
                results.append(json.dumps(calculate_route(dep_lon_f, dep_lat_f, dest_lon_f, dest_lat_f)))
            except:
                results.append(None)

        response['success'] = True
        response['results'] = results
    except Exception as e:
        response['success'] = False
        response['error_msg'] = str(e)

    return json.dumps(response)


#---------------------------incoming Payload: -------------------------
# {
#   "user": "IAM:RootIdentity",
#   "cluster": "arn:aws:redshift:us-east-1:564098277445:cluster:serverless-564098277445-99facde4-523d-4c19-967a-25db69d95157",
#   "database": "dev",
#   "external_function": "f_calculate_route_n",
#   "query_id": 1402674,
#   "request_id": "516ab525-36c8-4c51-83ce-6eb9aa546f33",
#   "arguments": [
#     [
#       -83.028056,
#       42.579722,
#       -83.045833,
#       42.331389
#     ]
#   ],
#   "num_records": 1
# }