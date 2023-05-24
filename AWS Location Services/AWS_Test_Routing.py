import requests
import json

RouteAddress2API = "https://kpegrs7u60.execute-api.us-east-1.amazonaws.com/DEV"
data = {
  "user": "IAM:RootIdentity",
  "cluster": "arn:aws:redshift:us-east-1:564098277445:cluster:serverless-564098277445-99facde4-523d-4c19-967a-25db69d95157",
  "database": "dev",
  "external_function": "f_calculate_route_n",
  "query_id": 1402674,
  "request_id": "516ab525-36c8-4c51-83ce-6eb9aa546f33",
  "arguments": [
    [
      -83.028056,
      42.579722,
      -83.045833,
      42.331389
    ]
  ],
  "num_records": 1
}
response = requests.post(RouteAddress2API, json.dumps(data))
resp = json.loads(response.content)
print(resp)