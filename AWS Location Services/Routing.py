import os
import boto3
import json

CALCULATOR_NAME = 'MyRouteCalculator'
#arn:aws:geo:us-east-1:564098277445:route-calculator/MyRouteCalculator

location = boto3.client('location')

def lambda_handler(event, context):
    return {
        'status': 200,
        'body': json.dumps(location.calculate_route_matrix(
                CalculatorName=CALCULATOR_NAME,
                DeparturePositions=event['departure_positions'],
                DestinationPositions=event['destination_positions'])['RouteMatrix'])
    }

