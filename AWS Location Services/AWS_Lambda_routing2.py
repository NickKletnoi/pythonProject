import json
import boto3
import botocore
import logging
import os
import time

logger = logging.getLogger()
logger.setLevel(logging.INFO)

location = boto3.client("location", config=botocore.config.Config(user_agent="Databricks"))
# CALCULATOR_NAME = os.environ["routeCalculator"]
CALCULATOR_NAME = 'MyCalculatorHere'


# [dep_id, dep_lon, dep_lat], [[dest_id1, dest_lon1, dest_lat1], [dest_id2, dest_lon2, dest_lat2], ...]

# [[dep_id, dest_id1, drive_dist1, Drive_time1], [dep_id, dest_id2, drive_dist2, Drive_time2]]


def calculate_route(dep_list, dest_list):
    dest_list_f = [[x[1], x[2]] for x in dest_list]
    print(dest_list_f)
    print([dep_list[1], dep_list[2]])

    #   response = location.calculate_route_matrix(
    #          CalculatorName=CALCULATOR_NAME,
    #          DeparturePositions=[[dep_lon,dep_lat]] ,
    #          DestinationPositions=[[dest_lon,dest_lat]])

    response = location.calculate_route_matrix(CalculatorName=CALCULATOR_NAME,
                                               DeparturePositions=[[dep_list[1], dep_list[2]]],
                                               DestinationPositions=dest_list_f,
                                               DepartNow=False,
                                               DistanceUnit="Miles")

    # res = response["result"]
    # print(response.keys())
    # print(response["RouteMatrix"][0])

    i = 0
    resp_lst = []
    for res in response["RouteMatrix"][0]:
        res["dep_id"] = dep_list[0]
        res["dest_id"] = dest_list[i][0]
        resp_lst.append(res)
        i += 1
        # print(res)

    # print(resp_lst)

    # if len(response) >= 1:
    #     distance = response["Legs"][0]["Distance"]
    #     duration = response["Legs"][0]["DurationSeconds"]

    #     res = {
    #         "route_id": route_id,
    #         "Distance": distance,
    #         "Duration": duration
    #     }

    return resp_lst


def lambda_handler(event, context):
    try:
        t1 = time.time()
        response = dict()
        results = []
        response['success'] = True

        records = event["arguments"]

        for record in records:
            dep_list, dest_list = record
            print(dep_list)
            print(dest_list)
            # [100, -83.028056, 42.579722]
            # [[1001, -83.045833, 42.331389], [1002, -83.045833, 42.331389]]
            # [[[100, -83.028056, 42.579722], [[1001, -83.045833, 42.331389], [1002, -83.045833, 42.331389]]], [[200, -83.028056, 42.579722], [[2001, -83.045833, 42.331389], [2002, -83.045833, 42.331389]]]]
            try:
                results.append(calculate_route(dep_list, dest_list))
                print('call_routing')

            except Exception as e:
                print("parsing of route calculation failed")
                response['success'] = False
                response['error_msg'] = str(e)
                results.append(None)

        t2 = time.time()
        lambda_response_time = t2 - t1
        response['lambda_response_time'] = lambda_response_time
        response['results'] = results

        print(response)


    except Exception as e:
        print("parsing of argumments failed")
        response['success'] = False
        response['error_msg'] = str(e)

    return response

