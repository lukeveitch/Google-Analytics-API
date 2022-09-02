from queryAPI import queryAPI
import pandas as pd
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

n = 1

dimensions_source_medium = ['date', 'sourceMedium']
dimensions_page_path = ['date', 'pagePath']
dimensions_default_grouping = ['date', 'channelGrouping']
dimensions_device_category = ['date', 'deviceCategory']

metrics = ['sessions','bounceRate', 'pageviews', 'transactions', 'transactionsPerSession', 'transactionRevenue']

regex='!~\?|[0-9].*'

startDate = '2018-11-01'
startDate = datetime.strptime(startDate, '%Y-%m-%d')
endDate = startDate + relativedelta(months=n)

df_source_medium = []
df_page_path = []
df_default_grouping = []
df_device_category = []


upperbound = date.today() + relativedelta(months=4)

print("Downloading... :)")
while datetime.date(endDate) <  upperbound:
    
    #Source Medium
    df1 = queryAPI(dimensions_source_medium, metrics, startDate, endDate)
    df_source_medium.append(df1)
    
    #Defaul Grouping
    df2 = queryAPI(dimensions_default_grouping, metrics, startDate, endDate)
    df_default_grouping.append(df2)
    
    #Device Category
    df3 = queryAPI(dimensions_device_category, metrics, startDate, endDate)
    df_device_category.append(df3)
    
    #Page Path
    filt = True
    df4 = queryAPI(dimensions_page_path, metrics, startDate, endDate, filt, regex)
    df_page_path.append(df4)
    
    filt = False
    startDate = startDate + relativedelta(months=n)
    endDate = endDate + relativedelta(months=n)

df_source_medium_final = pd.concat(df_source_medium)
df_page_path_final = pd.concat(df_page_path)
df_default_grouping_final = pd.concat(df_default_grouping)
df_device_category_final = pd.concat(df_device_category)

df_source_medium_final.to_csv('sourceMedium.csv', index = False)
df_page_path_final.to_csv('pagePath.csv', index = False)
df_default_grouping_final.to_csv('defaultGrouping.csv', index = False)
df_device_category_final.to_csv('device_category.csv', index = False)

import os
path = os.getcwd()
print(f"The files have been downloaded successfully. They are here: {path}")




