# SPDX-License-Identifier: MIT-0
import boto3
import botocore
import json
import logging
import os
import time

index_name = os.environ["PLACE_INDEX"]
CALCULATOR_NAME = 'MyRouteCalculator'
logger = logging.getLogger()
logger.setLevel(logging.INFO)

location = boto3.client("location", config=botocore.config.Config(user_agent="Amazon Redshift"))

def route_address(dep_lat,dep_lon,dest_lat,dest_lon):
    try:
        dep_val = []
        dep_val.append([dep_lon, dep_lat])
        dest_val = []
        dest_val.append([dest_lon, dest_lat])

        try:
            t1 = time.time()
            response = location.calculate_route_matrix(
                 CalculatorName=CALCULATOR_NAME,
                 DeparturePositions=dep_val,
                 DestinationPositions=dest_val)
            t2 = time.time()
            logger.info("Geocode Time: %.3f" % (t2 - t1))
        except botocore.exceptions.ClientError as ce:
            logger.exception(ce.response)
            response = {
                ce.response["Error"]["Code"]: ce.response["Error"]["Message"]
            }
        except botocore.exceptions.ParamValidationError as pve:
            logger.exception(pve)
            response = {
                "ParamValidationError": str(pve)
            }
    except Exception as e:
        logger.exception(e)
        response = {
            "Exception": str(e)
        }

    logger.info(response)

    return response
