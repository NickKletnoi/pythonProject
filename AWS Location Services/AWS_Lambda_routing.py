import json
import boto3
import botocore
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

location = boto3.client("location", config=botocore.config.Config(user_agent="Amazon Redshift"))
CALCULATOR_NAME = 'MyRouteCalculator'


def route_address(dep_lon, dep_lat, dest_lon, dest_lat):
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
    response = dict()
    records = event["arguments"]
    results = []

    for record in records:
        dep_lon_f = float(record["dep_lon"])
        dep_lat_f = float(record["dep_lat"])
        dest_lon_f = float(record["dest_lon"])
        dest_lat_f = float(record["dest_lat"])

        results.append(json.dumps(route_address(dep_lon_f, dep_lat_f, dest_lon_f, dest_lat_f)))

    return json.dumps(results)

## incoming payload ##
# {
#   "arguments": [
#     {
#       "dep_lon": "-83.028056",
#       "dep_lat": "42.579722",
#       "dest_lon": "-83.045833",
#       "dest_lat": "42.331389"
#     },
#     {
#       "dep_lon": "-83.078057",
#       "dep_lat": "42.579722",
#       "dest_lon": "-83.065834",
#       "dest_lat": "42.331389"
#     }
#   ]
# }