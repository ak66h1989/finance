#----import----
from sqlite3 import *
conn = connect('C:\\Users\\ak66h_000\\Documents\\db\\summary.sqlite3')
c = conn.cursor()
import os
# import psycopg2
# conn = psycopg2.connect("dbname=公開資訊觀測站 user=postgres password=d03724008")
# cur = conn.cursor()

import requests
from bs4 import BeautifulSoup
from numpy import *
from pandas import *
from functools import *

get_option("display.max_rows")
get_option("display.max_columns")
set_option("display.max_rows", 100)
set_option("display.max_columns", 1000)
set_option('display.expand_frame_repr', False)

# def mymerge(x, y):
#     m = merge(x, y, how='outer')
#     return m
def mymerge(x, y):
    m = merge(x, y, on=[col for col in list(x) if col in list(y)], how='outer')
    return m

# ----create table from csv----

path = 'C:/Users/ak66h_000/Dropbox/webscrap/公開資訊觀測站/彙總報表/綜合損益表/'
os.chdir(path)
l = os.listdir()
for folder in l:
    print(folder)
    os.chdir(path+folder)
    ll = os.listdir()
    li=[]
    for i in ll:
        try:
            df = read_csv(i, encoding='cp950', index_col=False)
            # del df['年季']
            df.公司代號=df.公司代號.astype(str)
            df.季 = df.季.astype(int)
            # df.insert(0, '年', int(folder[9:13]))
            # df.insert(1, '季', int(folder[-1]))
            li.append(df)
        except Exception as e:
            print(e)
            print(i)
            pass
    df=reduce(mymerge, li)
    df.年 = df.年.astype(int)
    df.季 = df.季.astype(int)
    df.公司代號 = df.公司代號.astype(str)
    df = df.sort_values(['年', '季', '公司代號']).reset_index(drop=True)
    sql='ALTER TABLE `%s` RENAME TO `%s0`'%(folder, folder)
    c.execute(sql)
    sql = 'create table `%s` (`%s`, PRIMARY KEY (%s))' % (folder, '`,`'.join(list(df)), '`年`, `季`, `公司代號`')
    c.execute(sql)
    sql = 'insert into `%s`(`%s`) values(%s)' % (folder, '`,`'.join(list(df)), ','.join('?' * len(list(df))))
    c.executemany(sql, df.values.tolist())
    conn.commit()
