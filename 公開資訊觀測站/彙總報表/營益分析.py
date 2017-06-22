#----import----

from sqlite3 import *
conn = connect('C:\\Users\\ak66h_000\\Documents\\db\\summary.sqlite3')
c = conn.cursor()

from numpy import *
from pandas import *
from functools import *

#----update----
import re
import os
os.getcwd()
dir()
os.listdir()
path = 'C:/Users/ak66h_000/OneDrive/webscrap/公開資訊觀測站/彙總報表/營益分析/'
os.chdir(path)
l = os.listdir()[-1]
df = read_csv(l, encoding='cp950')
t = re.findall(r'\d', l)
t = str(int(t[0] + t[1] + t[2])+1911) + '/' + t[3] + t[4]
col1=list(df)
col=['年', '季']
df['年'], df['季']= int(t[0:4]), int(t[6])
df=df[col+col1]
df['公司名稱'] = df['公司名稱'].str.strip()
df['公司代號']=df['公司代號'].astype(str)
name=list(df)
for i in range(len(name)):
    name[i] =name[i].replace('(營業毛利)/(營業收入)', '')
    name[i] =name[i].replace('(營業利益)/(營業收入)', '')
    name[i] =name[i].replace('(稅前純益)/(營業收入)', '')
    name[i] =name[i].replace('(稅後純益)/(營業收入)', '')
df.columns = name
tablename='營益分析'
sql='insert into `%s`(`%s`) values(%s)'%(tablename, '`,`'.join(list(df)), ','.join('?'*len(list(df))))
c.executemany(sql, df.values.tolist())
conn.commit()

# ----reorgnize table----
df = read_sql_query("SELECT * from `%s`"%tablename, conn)
df['年'], df['季'], df['公司代號']= df['年'].astype(int), df['季'].astype(int), df['公司代號'].astype(str)
df=df.drop_duplicates(subset=['年','季', '公司代號']).sort_values(['年','季', '公司代號'])
sql='ALTER TABLE `%s` RENAME TO `%s0`'%(tablename, tablename)
c.execute(sql)
sql='create table `%s` (`%s`, PRIMARY KEY (%s))'%(tablename, '`,`'.join(list(df)), '`年`, `季`, `公司代號`')
c.execute(sql)
sql='insert into `%s`(`%s`) values(%s)'%(tablename, '`,`'.join(list(df)), ','.join('?'*len(list(df))))
c.executemany(sql, df.values.tolist())
conn.commit()
sql="drop table `%s0`"%tablename
c.execute(sql)
print('finish')
# def mymerge(x, y):
#     m = merge(x, y, how='outer')
#     return m
#
# import re
# import os
# os.getcwd()
# dir()
# os.listdir()
# path = 'C:/Users/ak66h_000/OneDrive/webscrap/公開資訊觀測站/彙總報表/營益分析/'
# os.chdir(path)
# l = os.listdir()
# L = []
# for i in l:
#     df = read_csv(i, encoding='cp950')
#     t = re.findall(r'\d', i)
#     t = str(int(t[0] + t[1] + t[2])+1911) + '/' + t[3] + t[4]
#     d = {'年季': repeat(t, len(df))}
#     df1 = DataFrame(d)
#     df = concat([df1, df], axis=1)
#     df['公司代號'], df['公司名稱'] = df['公司代號'].str.strip(), df['公司名稱'].str.strip()
#     L.append(df)
# df1 = reduce(mymerge, L)
# name=list(df1)
# for i in range(len(name)):
#     name[i] =name[i].replace('(營業毛利)/(營業收入)', '')
#     name[i] =name[i].replace('(營業利益)/(營業收入)', '')
#     name[i] =name[i].replace('(稅前純益)/(營業收入)', '')
#     name[i] =name[i].replace('(稅後純益)/(營業收入)', '')
# df1.columns=name
# df1=df1.sort_values(['年季','公司代號'],ascending=[True,True])
# # ----create table----
# names = list(df1)
# c = conn.cursor()
# sql = "create table `" + '營益分析' + "`(" + "'" + names[0] + "'"
# for n in names[1:len(names)]:
#     sql = sql + ',' + "'" + n + "'"
# sql = sql + ',PRIMARY KEY (`年季`,`公司代號`))'
# c.execute(sql)
# # ----inserting data----
# sql = 'INSERT INTO `' + '營益分析' + '` VALUES (?'
# n = [',?'] * (len(names) - 1)
# for h in n:
#     sql = sql + h
# sql = sql + ')'
# c.executemany(sql, df1.values.tolist())
# conn.commit()
# print('done')

#---read from sqlite---

# tablename='營益分析'
# conn = connect('C:\\Users\\ak66h_000\\Documents\\summary.sqlite3')
# c = conn.cursor()
# df = read_sql_query("SELECT * from `%s`"%tablename, conn)
# df.年=df.年.astype(int)
# df.季=df.季.astype(int)
# df.公司代號=df.公司代號.astype(int)
# sql='ALTER TABLE `%s` RENAME TO `%s0`'%(tablename, tablename)
# c.execute(sql)
# sql='create table `%s` (`%s`, PRIMARY KEY (%s))'%(tablename, '`,`'.join(list(df)), '`年`, `季`, `公司代號`')
# c.execute(sql)
# sql='insert into `%s`(`%s`) values(%s)'%(tablename, '`,`'.join(list(df)), ','.join('?'*len(list(df))))
# c.executemany(sql, df.values.tolist())
# conn.commit()
# sql="drop table `%s0`"%tablename
# c.execute(sql)
# print('finish')

# path='C:/Users/ak66h_000/OneDrive/webscrap/公開資訊觀測站/彙總報表/%s/'%tablename
# os.chdir(path)
# L=os.listdir()
# df = read_csv(L[-1], encoding='cp950')
# name=list(df)
# for i in range(len(name)):
#     name[i] =name[i].replace('(營業毛利)/(營業收入)', '')
#     name[i] =name[i].replace('(營業利益)/(營業收入)', '')
#     name[i] =name[i].replace('(稅前純益)/(營業收入)', '')
#     name[i] =name[i].replace('(稅後純益)/(營業收入)', '')
# df.columns=name
# df.年=df.年.astype(int)
# df.季=df.季.astype(int)
#
# df.公司代號=df.公司代號.astype(int)
# sql='insert into `%s`(`%s`) values(%s)'%(tablename, '`,`'.join(list(df)), ','.join('?'*len(list(df))))
# c.executemany(sql, df.values.tolist())
# conn.commit()


# name=list(df)
# for i in range(len(name)):
#     name[i] =name[i].replace('(營業毛利)/(營業收入)', '')
#     name[i] =name[i].replace('(營業利益)/(營業收入)', '')
#     name[i] =name[i].replace('(稅前純益)/(營業收入)', '')
#     name[i] =name[i].replace('(稅後純益)/(營業收入)', '')
# df1.columns=name
# df1=df1.sort_values(['年季','公司代號'],ascending=[True,True])
# # ----create table----
# names = list(df1)
# c = conn.cursor()
# sql = "create table `" + '營益分析' + "`(" + "'" + names[0] + "'"
# for n in names[1:len(names)]:
#     sql = sql + ',' + "'" + n + "'"
# sql = sql + ',PRIMARY KEY (`年季`,`公司代號`))'
# c.execute(sql)
# # ----inserting data----
# sql = 'INSERT INTO `' + '營益分析' + '` VALUES (?'
# n = [',?'] * (len(names) - 1)
# for h in n:
#     sql = sql + h
# sql = sql + ')'
# c.executemany(sql, df1.values.tolist())
# conn.commit()
# print('done')



