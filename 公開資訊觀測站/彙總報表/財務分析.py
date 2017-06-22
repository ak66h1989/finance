#----import----

from sqlite3 import *
conn = connect('C:\\Users\\ak66h_000\\Documents\\TEJ.sqlite3')
c = conn.cursor()

from numpy import *
from pandas import *
from functools import *

def mymerge(x, y):
    m = merge(x, y, how='outer')
    return m

import re
import os
os.getcwd()
dir()
os.listdir()
path = 'C:/Users/ak66h_000/OneDrive/webscrap/公開資訊觀測站/彙總報表/財務分析/'
os.chdir(path)
l = os.listdir()
L = []
for i in l:
    df = read_csv(i, encoding='cp950')
    t = re.findall(r'\d', i)
    t = str(int(t[0] + t[1] + t[2])+1911)
    d = {'年': repeat(t, len(df))}
    df1 = DataFrame(d)
    df = concat([df1, df], axis=1)
    df['公司代號'], df['公司簡稱'] = df['公司代號'].str.strip(), df['公司簡稱'].str.strip()
    L.append(df)
df1 = reduce(mymerge, L)
name=list(df1)
for i in range(len(name)):
    name[i] = name[i].replace('財務結構-', '')
    name[i] = name[i].replace('償債能力-', '')
    name[i] = name[i].replace('經營能力-', '')
    name[i] = name[i].replace('獲利能力-', '')
    name[i] = name[i].replace('現金流量-', '')
    name[i] = name[i].replace('<br>', '')
df1.columns=name
df1=df1.sort_values(['年','公司代號'],ascending=[True,True])
# ----create table----
names = list(df1)
c = conn.cursor()
sql = "create table `" + '財務分析' + "`(" + "'" + names[0] + "'"
for n in names[1:len(names)]:
    sql = sql + ',' + "'" + n + "'"
sql = sql + ',PRIMARY KEY (`年`,`公司代號`))'
c.execute(sql)
# ----inserting data----
sql = 'INSERT INTO `' + '財務分析' + '` VALUES (?'
n = [',?'] * (len(names) - 1)
for h in n:
    sql = sql + h
sql = sql + ')'
c.executemany(sql, df1.values.tolist())
conn.commit()
print('done')

tablename='財務分析'
conn = connect('C:\\Users\\ak66h_000\\Documents\\summary.sqlite3')
c = conn.cursor()
df = read_sql_query("SELECT * from `%s`"%tablename, conn)
df.年=df.年.astype(int)
df.公司代號=df.公司代號.astype(int)
sql='ALTER TABLE `%s` RENAME TO `%s0`'%(tablename, tablename)
c.execute(sql)
sql='create table `%s` (`%s`, PRIMARY KEY (%s))'%(tablename, '`,`'.join(list(df)), '`年`, `公司代號`')
c.execute(sql)
sql='insert into `%s`(`%s`) values(%s)'%(tablename, '`,`'.join(list(df)), ','.join('?'*len(list(df))))
c.executemany(sql, df.values.tolist())
conn.commit()
sql="drop table `%s0`"%tablename
c.execute(sql)
print('finish')
