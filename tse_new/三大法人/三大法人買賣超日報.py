##----- pe is '0.00' when pe < 0 -----

from sqlite3 import *
connLite = connect('C:\\Users\\user\\Documents\\db\\tse.sqlite3')
c = connLite.cursor()


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
import crawler as crawler

get_option("display.max_rows")
get_option("display.max_columns")
set_option("display.max_rows", 100)
set_option("display.max_columns", 1000)
set_option('display.expand_frame_repr', False)
set_option('display.unicode.east_asian_width', True)


def getPlainText(lastdate, t, type):
    dateTime = lastdate + datetime.timedelta(days=t + 1)
    month, day = dateTime.month, dateTime.day
    if len(str(month)) == 1:
        month = '0' + str(month)
    if len(str(day)) == 1:
        day = '0' + str(day)
    input_date = str(dateTime.year) + str(month) + str(day)
    url = 'http://www.twse.com.tw/fund/T86?response=json&date={}&selectType={}'.format(input_date, type)
    print(dateTime.year, dateTime.month, dateTime.day, input_date)
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
    payload = {'headers':headers}
    plain_text = crawler.plainText(url, payload)
    return plain_text.get()


lastdate = datetime.datetime(2012, 5, 1)  #last time 9/03
delta = datetime.datetime.now() - lastdate


###----三大法人買賣超日報----
#----update using datetime----
tablename='三大法人買賣超日報'
for t in range(delta.days):
    try:

        d = json.loads(getPlainText(lastdate, t, 'ALL'))
        # for k, v in d.items():
        #     print(k, v)
        data = d['data']
        fields = d['fields']
        fields = [s.replace('</br>', '') for s in fields]
        date = d['date'][0:4] + '-' + d['date'][4:6] + '-' + d['date'][6:]
        df = DataFrame(data, columns=fields).replace(',', '', regex=True).replace('--', 'NaN').replace('</br>', '', regex=True)
        df.insert(0, '年月日', date)
        df['年月日'] = to_datetime(df['年月日']).astype(str)
        df['證券名稱'] = df['證券名稱'].str.strip()
        if '自營商買進股數' in list(df):
            floatColumns = ['外資買進股數', '外資賣出股數', '外資買賣超股數', '投信買進股數', '投信賣出股數', '投信買賣超股數', '自營商買賣超股數', '自營商買進股數', '自營商賣出股數', '三大法人買賣超股數']
            df[floatColumns] = df[floatColumns].astype(float)
        else:
            floatColumns = ['外資買進股數', '外資賣出股數', '外資買賣超股數', '投信買進股數', '投信賣出股數', '投信買賣超股數', '自營商買進股數(自行買賣)', '自營商賣出股數(自行買賣)', '自營商買賣超股數(自行買賣)', '自營商買進股數(避險)', '自營商賣出股數(避險)', '自營商買賣超股數(避險)', '三大法人買賣超股數']
            df[floatColumns] = df[floatColumns].astype(float)
        # df.dtypes
        columns = list(df)
        sqlCommand.insertData(tablename, df, connLite)
    except Exception as e:
        print('Exception: ', e)
        pass
