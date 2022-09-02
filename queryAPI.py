#!pip install --upgrade google-api-python-client
try:
    from oauth2client.service_account import ServiceAccountCredentials
    from apiclient.discovery import build
    import httplib2
    import pandas as pd
except ModuleNotFoundError:
    print("\n\nHello Pavani, there has been an error with the module depencies. \nPlease input the commands: pip install --upgrade google-api-python-client\n\npip install pandas\n\npip install oauth2client")

#Client_secrets.json file to access the API

credentials = ServiceAccountCredentials.from_json_keyfile_name(r"C:\Users\Moda\Desktop\client_secrets.json.json", ['https://www.googleapis.com/auth/analytics.readonly'])

http = credentials.authorize(httplib2.Http())
service = build('analytics', 'v4', http=http, discoveryServiceUrl=('https://analyticsreporting.googleapis.com/$discovery/rest'))

def queryAPI(dimensions, metrics, startDate, endDate, filt = False, regex = ''):
    startDate = startDate.strftime('%Y-%m-%d')
    endDate = endDate.strftime('%Y-%m-%d')
    
    if filt == True:
        query={
                'reportRequests': [
                    {
                        'viewId': '92459159',
                        'dateRanges': [{'startDate': f'{startDate}', 'endDate': f'{endDate}'}],
                        'metrics': [{'expression': f'ga:{metrics[0]}'},
                                    {'expression': f'ga:{metrics[1]}'},
                                    {'expression': f'ga:{metrics[2]}'},
                                    {'expression': f'ga:{metrics[3]}'},
                                    {'expression': f'ga:{metrics[4]}'}, 
                                    {'expression': f'ga:{metrics[5]}'}],

                        'dimensions': [{"name": f"ga:{dimensions[0]}"}, 
                                       {"name": f"ga:{dimensions[1]}"}],
                        "filtersExpression":f"ga:pagePath{regex}",
                        'pageSize': 10000
                    }]
            }
    else:
        query={
                'reportRequests': [
                    {
                        'viewId': '92459159',
                        'dateRanges': [{'startDate': f'{startDate}', 'endDate': f'{endDate}'}],
                        'metrics': [{'expression': f'ga:{metrics[0]}'},
                                    {'expression': f'ga:{metrics[1]}'},
                                    {'expression': f'ga:{metrics[2]}'},
                                    {'expression': f'ga:{metrics[3]}'},
                                    {'expression': f'ga:{metrics[4]}'}, 
                                    {'expression': f'ga:{metrics[5]}'}],

                        'dimensions': [{"name": f"ga:{dimensions[0]}"}, 
                                       {"name": f"ga:{dimensions[1]}"}],
                        #"filtersExpression":f"ga:pagePath{regex}",
                        'pageSize': 10000
                    }]
            }

    response = service.reports().batchGet(body=query).execute()

    data = []

    #Extract Data
    for report in response.get('reports', []):

        columnHeader = report.get('columnHeader', {})
        dimensionHeaders = columnHeader.get('dimensions', [])
        metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
        rows = report.get('data', {}).get('rows', [])

        for row in rows:

            dimensions = row.get('dimensions', [])
            dateRangeValues = row.get('metrics', [])

            for i, values in enumerate(dateRangeValues):
                for metricHeader, value in zip(metricHeaders, values.get('values')):
                    dimensions.append(float(value))

            data.append(dimensions)
    
    names= []
    for keys in query['reportRequests']:
        dimensions = keys['dimensions']
        for dct in dimensions:
            for key, value in dct.items():
                names.append(value[3:])
        metrics = keys['metrics']
        for dct in metrics:
            for key, value in dct.items():
                names.append(value[3:])
    
    df = pd.DataFrame(data, columns = names)
    df['date'] =  pd.to_datetime(df['date'], format='%Y-%m-%d')
    return df
        