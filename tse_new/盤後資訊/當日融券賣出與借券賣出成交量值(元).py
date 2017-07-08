##----- pe is '0.00' when pe < 0 -----

from sqlite3 import *
connLite = connect('C:\\Users\\user\\Documents\\db\\tse.sqlite3')
c = connLite.cursor()

import requests
from bs4 import BeautifulSoup
from numpy import *
from pandas import *
import re
import datetime
import json
import sys, os
sys.path.append('C:/Users/user/Dropbox/program/mypackage')
import psycopg2
import sqlCommand as sqlCommand

get_option("display.max_rows")
get_option("display.max_columns")
set_option("display.max_rows", 100)
set_option("display.max_columns", 1000)
set_option('display.expand_frame_repr', False)
set_option('display.unicode.east_asian_width', True)

def f():
    dateTime = lastdate + datetime.timedelta(days=t + 1)
    month, day = dateTime.month, dateTime.day
    if len(str(month)) == 1:
        month = '0' + str(month)
    if len(str(day)) == 1:
        day = '0' + str(day)
    input_date = str(dateTime.year) + str(month) + str(day)
    url = 'http://www.twse.com.tw/exchangeReport/TWTASU?response=json&date={}'.format(input_date)
    print(dateTime.year, dateTime.month, dateTime.day, input_date)
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
    source_code = requests.get(url, headers=headers)
    source_code.encoding = 'utf-8'
    plain_text = source_code.text

    return json.loads(plain_text)


# ---- update ----
lastdate = datetime.datetime(2016, 5, 31)  #last time 9/03
delta = datetime.datetime.now() - lastdate

###----tablename = '當日融券賣出與借券賣出成交量值(元)'----
#----update using datetime----
tablename = '當日融券賣出與借券賣出成交量值(元)'
for t in range(delta.days):
    try:
        d = f()

        # for k, v in d.items():
        #     print(k, v)

        data = d['data']
        fields = d['fields']
        date = d['date'][0:4] + '-' + d['date'][4:6] + '-' + d['date'][6:]

        df = DataFrame(data, columns=fields).replace(',', '', regex=True).replace('--', 'NaN').replace('-', 'NaN')
        df = df[df.證券名稱 != '合計']
        df.insert(0, '證券代號', df['證券名稱'].str.split().str[0].str.strip())
        df['證券名稱'] = df['證券名稱'].str.split().str[1].str.strip()
        df.insert(0, '年月日', date)
        df['年月日'] = to_datetime(df['年月日']).astype(str)

        floatColumns = ['融券賣出成交金額', '借券賣出成交金額']
        df[floatColumns] = df[floatColumns].astype(float)
        intColumns = ['融券賣出成交數量', '借券賣出成交數量']
        df[intColumns] = df[intColumns].astype(int)
        columns = list(df)
        sqlCommand.insertData(tablename, df, connLite)
    except Exception as e:
        print('Exception: ', e)
        pass