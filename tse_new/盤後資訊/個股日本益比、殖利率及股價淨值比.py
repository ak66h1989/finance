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

def f(type):
    dateTime = lastdate + datetime.timedelta(days=t + 1)
    month, day = dateTime.month, dateTime.day
    if len(str(month)) == 1:
        month = '0' + str(month)
    if len(str(day)) == 1:
        day = '0' + str(day)
    input_date = str(dateTime.year) + str(month) + str(day)
    url = 'http://www.twse.com.tw/exchangeReport/BWIBBU_d?response=json&date={}&selectType={}'.format(input_date, type)
    print(dateTime.year, dateTime.month, dateTime.day, input_date)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}

    source_code = requests.get(url, headers=headers)

    source_code.encoding = 'utf-8'
    plain_text = source_code.text

    return json.loads(plain_text)


#  ---- create table ----
lastdate = datetime.datetime(2017, 6, 21)  #last time 9/03
delta = datetime.datetime.now() - lastdate
tablename = '個股日本益比、殖利率及股價淨值比'

t=2
d = f('ALL')

# for k, v in d.items():
#     print(k, v)

data = d['data']
fields = d['fields']
date = d['date'][0:4] + '-' + d['date'][4:6] + '-' + d['date'][6:]

df = DataFrame(data, columns=fields).replace(',', '', regex=True).replace('--', 'NaN').replace('-', 'NaN')
df.insert(0, '年月日', date)
df['年月日'] = to_datetime(df['年月日']).astype(str)
floatColumns = ['殖利率(%)', '本益比', '股價淨值比']
intColumns = ['股利年度']
df[intColumns] = df[intColumns].astype(int)
df[floatColumns] = df[floatColumns].astype(float)
df.股利年度 = df.股利年度 + 1911
df['財報年/季'] = (df['財報年/季'].str.split('/').str[0].astype(int) + 1911).astype(str) + '/' + df['財報年/季'].str.split('/').str[1]
# df.dtypes
columns = list(df)

tablename_new = tablename + '0'
sqlCommand.renameTable(tablename, tablename_new, connLite)
columns = list(df)
primaryKeys = ['年月日', '證券代號']
sqlCommand.createTable(tablename, columns, primaryKeys, connLite)
# sqlCommand.insertData(tablename, df, connLite)
# sqlCommand.dropTable(tablename_new, connLite)

# ---- update ----
lastdate = datetime.datetime(2005, 9, 1)  #last time 9/03
delta = datetime.datetime.now() - lastdate

###----tablename = '個股日本益比、殖利率及股價淨值比'----
#----update using datetime----
tablename = '個股日本益比、殖利率及股價淨值比'
for t in range(delta.days):
    try:
        d = f('ALL')

        # for k, v in d.items():
        #     print(k, v)

        data = d['data']
        fields = d['fields']
        date = d['date'][0:4] + '-' + d['date'][4:6] + '-' + d['date'][6:]

        df = DataFrame(data, columns=fields).replace(',', '', regex=True).replace('--', 'NaN').replace('-', 'NaN')
        df['證券代號'] = df['證券代號'].str.strip()
        df['證券名稱'] = df['證券名稱'].str.strip()
        df.insert(0, '年月日', date)
        df['年月日'] = to_datetime(df['年月日']).astype(str)
        floatColumns = ['殖利率(%)', '本益比', '股價淨值比']
        df[floatColumns] = df[floatColumns].astype(float)
        columns = ['年月日', '證券代號', '證券名稱', '殖利率(%)', '股利年度', '本益比', '股價淨值比', '財報年/季']
        if '股利年度' and '財報年/季' in list(df):
            intColumns = ['股利年度']
            df[intColumns] = df[intColumns].astype(int)
            df[floatColumns] = df[floatColumns].astype(float)
            df.股利年度 = df.股利年度 + 1911
            df['財報年/季'] = (df['財報年/季'].str.split('/').str[0].astype(int) + 1911).astype(str)+'/' + df['財報年/季'].str.split('/').str[1]
            # df.dtypes
            df = df[columns]
            sqlCommand.insertData(tablename, df, connLite)
        elif '財報年/季' in list(df):
            df['股利年度'] = 'NaN'
            df['財報年/季'] = (df['財報年/季'].str.split('/').str[0].astype(int) + 1911).astype(str)+'/' + df['財報年/季'].str.split('/').str[1]
            df = df[columns]
            sqlCommand.insertData(tablename, df, connLite)
        else:
            df['股利年度'] = 'NaN'
            df['財報年/季'] = 'NaN'
            df = df[columns]
            sqlCommand.insertData(tablename, df, connLite)
    except Exception as e:
        print('Exception: ', e)
        pass