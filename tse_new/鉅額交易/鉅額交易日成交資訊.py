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
import crawler_fp as crawler_fp

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
    url = 'http://www.twse.com.tw/block/BFIAUU?response=json&date={}&selectType={}'.format(input_date, type)
    print(dateTime.year, dateTime.month, dateTime.day, input_date)
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
    payload = {'headers':headers}
    plain_text = crawler.plainText(url, payload)
    return plain_text.get()


# lastdate = datetime.datetime(2005, 4, 10)  #last time 9/03
lastdate = datetime.datetime(2005, 4, 3)  #last time 9/03
delta = datetime.datetime.now() - lastdate

# ---- create table ----

# --get all columns --
# tablename='鉅額交易日成交資訊'
# cols = []
# for t in range(delta.days):
#     try:
#
#         d = json.loads(getPlainText(lastdate, t, 'S'))
#         data = d['data']
#         fields = d['fields']
#         date = d['date'][0:4] + '-' + d['date'][4:6] + '-' + d['date'][6:]
#         df = DataFrame(data, columns=fields).replace(',', '', regex=True).replace('--', 'NaN').replace('', 'NaN')
#         df.insert(0, '年月日', date)
#         df['年月日'] = to_datetime(df['年月日']).astype(str)
#
#         cols = cols + list(df)
#         columns = set(cols)
#         print(columns)
#         cols = list(columns)
#     except Exception as e:
#         print('Exception: ', e)
#         pass

# columns = ['年月日', '證券代號', '證券名稱', '交易別', '成交價', '成交股數', '成交量', '成交金額', '交割期別', '第幾筆']
# primaryKeys = ['年月日', '證券代號', '第幾筆']
# sqlCommand.createTable(tablename, columns, primaryKeys, connLite)

###----鉅額交易日成交資訊----
#----update using datetime----
tablename='鉅額交易日成交資訊'

# -- 1 company in 1 day may have more than 1 transaction --
def f1(df):
    df.第幾筆 = list(range(1,len(df.第幾筆)+1))
    return df


def gen(max):
    global t, df
    t = 0
    while t <= max:
        try:
            d = json.loads(getPlainText(lastdate, t, 'S'))
            # for k, v in d.items():
            #     print(k, v)
            data = d['data']
            fields = d['fields']
            date = d['date'][0:4] + '-' + d['date'][4:6] + '-' + d['date'][6:]
            df = DataFrame(data, columns=fields).replace(',', '', regex=True).replace('--', 'NaN').replace('', 'NaN')
            df.insert(0, '年月日', date)
            df.insert(len(list(df)), '第幾筆', 1)
            df['年月日'] = to_datetime(df['年月日']).astype(str)
            intColumns = ['第幾筆']
            floatColumns = []
            for col in ['成交價', '成交股數', '成交金額', '成交量']:
                if col in list(df):
                    floatColumns.append(col)
            df[floatColumns] = df[floatColumns].astype(float)
            df[intColumns] = df[intColumns].astype(int)
            df = df.groupby(['年月日', '證券代號']).apply(f1)
            yield t, sqlCommand.insertData(tablename, df, connLite)
            t = t + 1
        except Exception as e:
            print('Exception: ', e)
            pass


g = gen(delta.days)
# next(g)
for _ in g: pass

# def f(s):
#     return list(range(1,len(s)+1))
# def f1(df):
#     df.第幾筆 = list(range(1,len(df.第幾筆)+1))
#     return df
# df['第幾筆'] = df.groupby(['年月日', '證券代號'])['第幾筆'].apply(f).reset_index(drop=True)
# df.groupby(['年月日', '證券代號']).apply(f1)
