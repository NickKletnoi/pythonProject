import json
import boto3
import botocore
location = boto3.client("location", config=botocore.config.Config(user_agent="Amazon Redshift"))
CALCULATOR_NAME = 'MyRouteCalculator'

def route_address(dep_lat,dep_lon,dest_lat,dest_lon):
        dep_val = []
        dep_val.append([dep_lon, dep_lat])
        dest_val = []
        dest_val.append([dest_lon, dest_lat])

        response = location.calculate_route_matrix(
                 CalculatorName=CALCULATOR_NAME,
                 DeparturePositions=dep_val,
                 DestinationPositions=dest_val)
        return response

def lambda_handler(event, context):

        records = event["arguments"]
        results = []

        for record in records:
            dep_lat,dep_lon,dest_lat,dest_lon = record
            results.append(
                json.dumps(route_address(dep_lat,dep_lon,dest_lat,dest_lon)
                           ))
        return results



