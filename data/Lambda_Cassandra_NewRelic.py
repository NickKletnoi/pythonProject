# import sessions
import json
from datetime import datetime
import requests
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
import time
from ssl import SSLContext
from ssl import CERT_REQUIRED
from ssl import PROTOCOL_TLSv1_2
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider


def lambda_handler(event, context):
    total_namespace = []
    total_max_result = []
    start_time_total = []
    end_time_total = []
    total_inspectedcount_result = []
    total_accounts = []
    cassandra_db_session = None
    cassandra_db_username = 'Venkat-at-595363153044'
    cassandra_db_password = '6dOJzR7D+kJhviHZMhy7WbmVvxw/USu+DRbIHcqY+jw='
    cassandra_db_endpoints = ['cassandra.us-east-1.amazonaws.com']
    cassandra_db_port = 9142
    ssl_context = SSLContext(PROTOCOL_TLSv1_2)
    ssl_context.load_verify_locations('AmazonRootCA1.pem')
    ssl_context.verify_mode = CERT_REQUIRED
    auth_provider = PlainTextAuthProvider(username=cassandra_db_username, password=cassandra_db_password)
    cluster = Cluster(cassandra_db_endpoints, ssl_context=ssl_context, auth_provider=auth_provider,
                      port=cassandra_db_port)
    session = cluster.connect('test_new')

    headers_1 = {
        'Accept': 'application/json',
        'X-Query-Key': 'NRIQ-aLF9e6Fb2fU5Q2UDO96fQ_aahYgEHmLp',
    }

    headers_2 = {
        'Accept': 'application/json',
        'X-Query-Key': 'NRIQ-64nOXd_tIJWuiHMHIspmvkhPqtMXAx64',
    }

    headers_3 = {
        'Accept': 'application/json',
        'X-Query-Key': 'NRIQ-aLF9e6Fb2fU5Q2UDO96fQ_aahYgEHmLp',
    }

    params_1 = (
        ('nrql',
         'FROM Metric SELECT max(newrelic.timeslice.value) as \'ThreadActive\' WHERE ( appName like \'%B6VV%GSS%\') and appName not like \'%PILOT%\' and metricTimesliceName like \'JmxBuiltIn/ThreadPool%https-jsse-nio%\' WITH METRIC_FORMAT \'JmxBuiltIn/ThreadPool/{thread}/Active\' facet appName, cases(WHERE appName like \'%PR-TDC%\' as \'TDC\',WHERE appName like \'%PR-B2B-TDC%\' as \'TDC-B2B\',WHERE appName like \'%PR-B2B%\' as \'SDC-B2B\',WHERE appName like \'%PR-SDC%\' as \'SDC\',WHERE appName like \'%PR-AE%\' as \'AWS-E\',WHERE appName like \'%PR-AW%\' as \'AWS-W\') as Data_Center,host.displayName limit max timeseries 1 minute since 7 minutes ago until 2 minutes ago'),
    )

    params_2 = (
        ('nrql',
         'SELECT percentile(service.responseTime, 99) FROM Metric where appName like \'%EV6V%prod-ekse\' facet appName since 8 minutes ago until 3 minutes ago timeseries 1 minute'),
    )

    params_3 = (
        ('nrql',
         'FROM K8sContainerSample SELECT max(cpuCoresUtilization) where clusterName like \'%ev6v-pr%e\' and podName like \'cxp-%\' facet namespace,clusterName,podName limit max since 6 minutes ago until 1 minute ago timeseries'),
    )

    responce_2 = requests.get('https://insights-api.newrelic.com/v1/accounts/3136945/query', headers=headers_2,
                              params=params_2)

    response_1 = requests.get('https://insights-api.newrelic.com/v1/accounts/1135888/query', headers=headers_1,
                              params=params_1)

    response_3 = requests.get('https://insights-api.newrelic.com/v1/accounts/1135888/query', headers=headers_3,
                              params=params_3)

    # cpu = response.json()
    # print(json.dumps(cpu))
    res_1 = response_1.json()
    res_2 = responce_2.json()
    res_3 = response_3.json()

    total_len = len(res_3['facets'])
    # timeseries_len = len(cpu['facets'][0]['timeSeries'])
    # timeseries_len = len(res_1['totalResult']['timeSeries'])

    beginTimeSeconds = res_1['totalResult']['total']['beginTimeSeconds']
    endTimeSeconds = res_1['totalResult']['total']['endTimeSeconds']
    inspectedCount = res_1['totalResult']['total']['inspectedCount']
    percentiles = res_1['totalResult']['total']['results'][0]['max']
    account = res_1['metadata']['accounts'][0]
    beginTimeSeconds_2 = res_2['totalResult']['total']['beginTimeSeconds']
    endTimeSeconds_2 = res_2['totalResult']['total']['endTimeSeconds']
    inspectedCount_2 = res_2['totalResult']['total']['inspectedCount']
    max_2 = str(res_2['totalResult']['total']['results'][0]['percentiles']['99'])
    account_2 = res_2['metadata']['accounts'][0]
    percentiles_str = str(percentiles)
    start_time = [beginTimeSeconds, beginTimeSeconds_2]
    end_time = [endTimeSeconds, endTimeSeconds_2]
    inspectedCount_all = [inspectedCount, inspectedCount_2]
    result = [percentiles, max_2]
    app_name = ['Appname_1', 'Appname_2']
    accounts = [account, account_2]

    for i in range(0, total_len):
        total_namespace.append(res_3['facets'][i]['name'][2])
        total_max_result.append(str((list(res_3['facets'][i]['total']['results'][0].values())[0])))
        start_time_total.append(res_3['facets'][i]['total']['beginTimeSeconds'])
        print(res_3['facets'][i]['name'][0])
        end_time_total.append(res_3['facets'][i]['total']['endTimeSeconds'])
        total_inspectedcount_result.append(res_3['facets'][i]['total']['inspectedCount'])
        total_accounts.append(res_3['metadata']['accounts'][0])

    for i in range(0, 2):
        query = SimpleStatement(
            "INSERT INTO test_new.fi_test (start_time, end_time, inspectedcount, result, app_name, account) VALUES (%s, %s, %s, %s, %s, %s)",
            consistency_level=ConsistencyLevel.LOCAL_QUORUM)
        session.execute(query, (start_time[i], end_time[i], inspectedCount_all[i], result[i], app_name[i], accounts[i]))

    for i in range(len(total_namespace)):
        query = SimpleStatement(
            "INSERT INTO test_new.fi_test (start_time, end_time, inspectedcount, result, app_name, account) VALUES (%s, %s, %s, %s, %s, %s)",
            consistency_level=ConsistencyLevel.LOCAL_QUORUM)
        session.execute(query, (
        start_time_total[i], end_time_total[i], total_inspectedcount_result[i], total_max_result[i], total_namespace[i],
        total_accounts[i]))
        print(total_namespace[i])
    return print(len(total_namespace))