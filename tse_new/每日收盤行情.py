##----- pe is '0.00' when pe < 0 -----

from sqlite3 import *
connLite = connect('C:\\Users\\ak66h_000\\Documents\\db\\tse.sqlite3')
c = connLite.cursor()

import requests
from bs4 import BeautifulSoup
from numpy import *
from pandas import *
import re
import datetime
import json
import sys, os
sys.path.append('C:/Users/ak66h_000/Dropbox/program/mypackage')
import psycopg2
import sqlCommand as sqlCommand

get_option("display.max_rows")
get_option("display.max_columns")
set_option("display.max_rows", 100)
set_option("display.max_columns", 1000)
set_option('display.expand_frame_repr', False)
set_option('display.unicode.east_asian_width', True)

def f(type):
    dateTime = lastdate + datetime.timedelta(days=t + 1)
    month, day = dateTime.month, dateTime.day
    if len(str(month)) == 1:
        month = '0' + str(month)
    if len(str(day)) == 1:
        day = '0' + str(day)
    input_date = str(dateTime.year) + str(month) + str(day)
    url = 'http://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={}&type={}'.format(input_date, type)
    print(dateTime.year, dateTime.month, dateTime.day, input_date)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}

    source_code = requests.get(url, headers=headers)

    source_code.encoding = 'utf-8'
    plain_text = source_code.text

    return json.loads(plain_text)


lastdate = datetime.datetime(2017, 5, 19)  #last time 9/03
delta = datetime.datetime.now() - lastdate

###----每日收盤行情(全部(不含權證、牛熊證))----
#----update using datetime----
tablename='每日收盤行情(全部(不含權證、牛熊證))'
for t in range(delta.days):
    try:

        d = f('ALLBUT0999')

        # for k, v in d.items():
        #     print(k, v)

        data5 = d['data5']
        fields5 = d['fields5']
        date = d['date'][0:4] + '-' + d['date'][4:6] + '-' + d['date'][6:]

        df = DataFrame(data5, columns=fields5).replace(',', '', regex=True).replace('--', 'NaN')
        df['漲跌(+/-)'] = df['漲跌(+/-)'].replace('<p style= color:red>+</p>', 1).replace('<p style= color:green>-</p>', -1).replace('X', 'NaN').replace(' ', 0)
        df.insert(0, '年月日', date)
        df['年月日'] = to_datetime(df['年月日']).astype(str)
        floatColumns = ['成交股數', '成交筆數', '成交金額', '開盤價', '最高價', '最低價', '收盤價', '漲跌(+/-)', '漲跌價差', '最後揭示買價', '最後揭示買量',
                        '最後揭示賣價', '最後揭示賣量', '本益比']
        # intColumns = []
        # df[intColumns] = df[intColumns].astype(int)
        df[floatColumns] = df[floatColumns].astype(float)
        # df.dtypes
        columns = list(df)
        sqlCommand.insertData(tablename, df, connLite)
    except Exception as e:
        print(e)
        pass


###----大盤統計資訊----
#----update using datetime----
tablename='大盤統計資訊'
for t in range(delta.days):
    try:

        d = f('ALLBUT0999')

        # for k, v in d.items():
        #     print(k, v)

        data1 = d['data1']
        fields1 = d['fields1']
        date = d['date'][0:4] + '-' + d['date'][4:6] + '-' + d['date'][6:]

        df = DataFrame(data1, columns=fields1).replace(',', '', regex=True).replace('--', 'NaN')
        df['漲跌(+/-)'] = df['漲跌(+/-)'].replace("<p style ='color:red'>+</p>", 1).replace("<p style ='color:green'>-</p>", -1).replace('X', 'NaN').replace(' ', 0)
        df.insert(0, '年月日', date)
        df['年月日'] = to_datetime(df['年月日']).astype(str)
        floatColumns = ['收盤指數', '漲跌(+/-)', '漲跌點數', '漲跌百分比(%)']
        # intColumns = []
        # df[intColumns] = df[intColumns].astype(int)
        df[floatColumns] = df[floatColumns].astype(float)
        # df.dtypes
        columns = list(df)
        sqlCommand.insertData(tablename, df, connLite)
    except Exception as e:
        print(e)
        pass



###----大盤統計資訊(報酬指數)----
# ----update using datetime----
tablename = '大盤統計資訊'
for t in range(delta.days):
    try:

        d = f('ALLBUT0999')

        for k, v in d.items():
            print(k, v)

        data2 = d['data2']
        fields2 = d['fields2']
        date = d['date'][0:4] + '-' + d['date'][4:6] + '-' + d['date'][6:]

        df = DataFrame(data2, columns=fields2).replace(',', '', regex=True).replace('--', 'NaN')
        df['漲跌(+/-)'] = df['漲跌(+/-)'].replace("<p style ='color:red'>+</p>", 1).replace("<p style ='color:green'>-</p>", -1).replace('X', 'NaN').replace(' ', 0)
        df.insert(0, '年月日', date)
        df = df.rename(columns={'報酬指數':'指數'})
        df['年月日'] = to_datetime(df['年月日']).astype(str)
        floatColumns = ['收盤指數', '漲跌(+/-)', '漲跌點數', '漲跌百分比(%)']
        # intColumns = []
        # df[intColumns] = df[intColumns].astype(int)
        df[floatColumns] = df[floatColumns].astype(float)
        # df.dtypes
        columns = list(df)
        sqlCommand.insertData(tablename, df, connLite)
    except Exception as e:
        print(e)
        pass


###----大盤成交統計----
# ----update using datetime----
tablename = '大盤成交統計'
for t in range(delta.days):
    try:

        d = f('ALLBUT0999')

        # for k, v in d.items():
        #     print(k, v)

        data3 = d['data3']
        fields3 = d['fields3']
        date = d['date'][0:4] + '-' + d['date'][4:6] + '-' + d['date'][6:]

        df = DataFrame(data3, columns=fields3).replace(',', '', regex=True).replace('--', 'NaN')
        df.insert(0, '年月日', date)
        df['年月日'] = to_datetime(df['年月日']).astype(str)
        floatColumns = ['成交金額(元)','成交股數(股)','成交筆數']
        # intColumns = []
        # df[intColumns] = df[intColumns].astype(int)
        df[floatColumns] = df[floatColumns].astype(float)
        # df.dtypes
        columns = list(df)
        sqlCommand.insertData(tablename, df, connLite)
    except Exception as e:
        print(e)
        pass


###----漲跌證券數合計----
# ----update using datetime----
tablename = '漲跌證券數合計'
for t in range(delta.days):
    try:

        d = f('ALLBUT0999')

        for k, v in d.items():
            print(k, v)

        data4 = d['data4']
        fields4 = d['fields4']
        date = d['date'][0:4] + '-' + d['date'][4:6] + '-' + d['date'][6:]

        data4[0][1].split('(')[0]
        L =[]
        l = data4[0]
        L.append([i.split('(')[0] for i in l])
        L.append([i.split('(')[1].replace(')', '') for i in l])
        l = data4[1]
        L.append([i.split('(')[0] for i in l])
        L.append([i.split('(')[1].replace(')', '') for i in l])
        L.append(data4[2])
        L.append(data4[3])
        L.append(data4[4])

        df = DataFrame(L, columns=fields4).replace(',', '', regex=True).replace('--', 'NaN')
        df.insert(0, '年月日', date)
        df['年月日'] = to_datetime(df['年月日']).astype(str)
        # floatColumns = []
        intColumns = ['整體市場', '股票']
        df[intColumns] = df[intColumns].astype(int)
        # df[floatColumns] = df[floatColumns].astype(float)
        # df.dtypes
        columns = list(df)
        sqlCommand.insertData(tablename, df, connLite)
    except Exception as e:
        print(e)
        pass


###----牛證(不含可展延牛證)----
# ----update using datetime----
tablename = '牛證(不含可展延牛證)'
for t in range(delta.days):
    try:

        d = f('0999C')

        # for k, v in d.items():
        #     print(k, v)

        data= d['data1']
        fields = d['fields1']
        date = d['date'][0:4] + '-' + d['date'][4:6] + '-' + d['date'][6:]

        df = DataFrame(data, columns=fields).replace(',', '', regex=True).replace('--', 'NaN')
        df.insert(0, '年月日', date)
        df['年月日'] = to_datetime(df['年月日']).astype(str)
        df['漲跌(+/-)'] = df['漲跌(+/-)'].replace("<p style= color:red>+</p>", 1).replace("<p style= color:green>-</p>", -1).replace('X', 'NaN').replace(' ', 0)

        df['牛熊證觸及限制價格'] = df['牛熊證觸及限制價格'].replace('', 'NaN').fillna('NaN')
        df['本益比'] = df['本益比'].replace('', 'NaN').fillna('NaN')
        intColumns = ['成交股數', '成交筆數', '最後揭示買量', '最後揭示賣量']
        floatColumns = ['成交金額', '開盤價', '最高價', '最低價', '收盤價', '漲跌(+/-)', '漲跌價差', '最後揭示買價', '最後揭示賣價', '本益比', '牛熊證觸及限制價格', '標的證券收盤價/指數']
        df[intColumns] = df[intColumns].astype(int)
        df[floatColumns] = df[floatColumns].astype(float)
        # df.dtypes
        columns = list(df)
        sqlCommand.insertData(tablename, df, connLite)
    except Exception as e:
        print('exception: ', e)
        pass

###----熊證(不含可展延熊證)----
# ----update using datetime----
tablename = '熊證(不含可展延熊證)'
for t in range(delta.days):
    try:

        d = f('0999B')

        # for k, v in d.items():
        #     print(k, v)

        data= d['data1']
        fields = d['fields1']
        date = d['date'][0:4] + '-' + d['date'][4:6] + '-' + d['date'][6:]

        df = DataFrame(data, columns=fields).replace(',', '', regex=True).replace('--', 'NaN')
        df.insert(0, '年月日', date)
        df['年月日'] = to_datetime(df['年月日']).astype(str)
        df['漲跌(+/-)'] = df['漲跌(+/-)'].replace("<p style= color:red>+</p>", 1).replace("<p style= color:green>-</p>", -1).replace('X', 'NaN').replace(' ', 0)

        df['牛熊證觸及限制價格'] = df['牛熊證觸及限制價格'].replace('', 'NaN').fillna('NaN')
        df['本益比'] = df['本益比'].replace('', 'NaN').fillna('NaN')
        intColumns = ['成交股數', '成交筆數', '最後揭示買量', '最後揭示賣量']
        floatColumns = ['成交金額', '開盤價', '最高價', '最低價', '收盤價', '漲跌(+/-)', '漲跌價差', '最後揭示買價', '最後揭示賣價', '本益比', '牛熊證觸及限制價格', '標的證券收盤價/指數']
        df[intColumns] = df[intColumns].astype(int)
        df[floatColumns] = df[floatColumns].astype(float)
        # df.dtypes
        columns = list(df)
        sqlCommand.insertData(tablename, df, connLite)
    except Exception as e:
        print('exception: ', e)
        pass


###----可展延牛證----
# ----update using datetime----
tablename = '可展延牛證'
for t in range(delta.days):
    try:

        d = f('0999X')

        # for k, v in d.items():
        #     print(k, v)

        data= d['data1']
        fields = d['fields1']
        date = d['date'][0:4] + '-' + d['date'][4:6] + '-' + d['date'][6:]

        df = DataFrame(data, columns=fields).replace(',', '', regex=True).replace('--', 'NaN')
        df.insert(0, '年月日', date)
        df['年月日'] = to_datetime(df['年月日']).astype(str)
        df['漲跌(+/-)'] = df['漲跌(+/-)'].replace("<p style= color:red>+</p>", 1).replace("<p style= color:green>-</p>", -1).replace('X', 'NaN').replace(' ', 0)

        df['牛熊證觸及限制價格'] = df['牛熊證觸及限制價格'].replace('', 'NaN').fillna('NaN')
        df['本益比'] = df['本益比'].replace('', 'NaN').fillna('NaN')
        intColumns = ['成交股數', '成交筆數', '最後揭示買量', '最後揭示賣量']
        floatColumns = ['成交金額', '開盤價', '最高價', '最低價', '收盤價', '漲跌(+/-)', '漲跌價差', '最後揭示買價', '最後揭示賣價', '本益比', '牛熊證觸及限制價格', '標的證券收盤價/指數']
        df[intColumns] = df[intColumns].astype(int)
        df[floatColumns] = df[floatColumns].astype(float)
        # df.dtypes
        columns = list(df)
        sqlCommand.insertData(tablename, df, connLite)
    except Exception as e:
        print('exception: ', e)
        pass