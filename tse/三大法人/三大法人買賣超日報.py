##三大法人買賣超日報 全部(不含權證、牛熊證、可展延牛熊證)

# sqlite3 can only run in console
from sqlite3 import *
conn = connect('C:\\Users\\ak66h_000\\Documents\\TEJ.sqlite3')
c = conn.cursor()

import requests
from bs4 import BeautifulSoup
from numpy import *
from pandas import *
from functools import *

def mymerge(x, y):
    m = merge(x, y, how='outer')
    return m

#----create table----
url = 'http://www.twse.com.tw/ch/trading/fund/T86/T86.php'
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
payload = {'input_date': '105/02/03', 'select2': 'ALLBUT0999', 'sorting': 'by_issue'}
source_code = requests.post(url, headers=headers, data=payload)  # should use data instead of params
source_code.encoding = 'big5'
plain_text = source_code.text
soup = BeautifulSoup(plain_text, 'html.parser')
date = soup.find_all('thead')[0].find_all('tr')[0].find_all('th')[0].string

h = ['年月日']
for th in soup.find_all('thead')[0].find_all('tr')[1].find_all('th'):
    h.append(th.text)
l = [h]
df = DataFrame(l)
df.columns = df.ix[0, :]
df = df.ix[1:len(df), :]
tablename0 =list(df)
names = list(df)
# sql = "create table `" + date.split()[1] + "`(" + "'" + names[0] + "'"
sql = "create table `" + '三大法人買賣超日報(股)' + "`(" + "'" + names[0] + "'"
for n in names[1:len(names)]:
    sql = sql + ',' + "'" + n + "'"
sql = sql + ',PRIMARY KEY (`年月日`,`證券代號`))'
c.execute(sql)

# y=['104','103','102','101']
# m=['12','11','10','09','08','07','06','05','04','03','02','01']
# d=['31', '30', '29','28','27','26','25','24', '23','22','21','20', '19', '18', '17', '16', '15','14', '13', '12', '11','10','09','08','07', '06','05','04','03', '02','01']

#----renew----

# def f1(y,m,d):
#     global df1
#     for year in y:
#         for month in m:
#             for day in d:
#                 try:
#                     url = 'http://www.twse.com.tw/ch/trading/fund/T86/T86.php'
#                     input_date=year+'/'+month+'/'+day
#                     payload = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36',
#                                'input_date': input_date, 'select2': 'ALLBUT0999','sorting': 'by_issue'}
#                     source_code = requests.post(url,params=payload)
#                     source_code.encoding = 'big5'
#                     plain_text = source_code.text
#                     soup = BeautifulSoup(plain_text, 'html.parser')
#                     date=soup.find_all('thead')[0].find_all('tr')[0].find_all('th')[0].string
#
#                     h=['年月日']
#                     for th in soup.find_all('thead')[0].find_all('tr')[1].find_all('th'):
#                         h.append(th.text)
#                     l=[h]
#                     for tr in soup.find_all('tbody')[0].find_all('tr'):
#                         r = [str(int(year) + 1911) + '/' + month + '/' + day]
#                         for td in tr.find_all('td'):
#                             r.append(td.string)
#                         l.append(r)
#                     df = DataFrame(l)
#                     df.columns=df.ix[0,:]
#                     df=df.ix[1:len(df),:]
#                     df = df.replace(',', '', regex=True)
#                     df.to_csv('C:/Users/ak66h_000/OneDrive/webscrap/tse/三大法人/三大法人/' + year + month + day + '.csv',index=False)
#                     print(date)
#                 except Exception as e:
#                     print(e)
#                     pass
#     print('1done')
#
# y=['101', '102']
# m=['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
# d=['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31']
# f1(y,m,d)
# y=['103']
# m=['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11']
# d=['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31']
# f1(y,m,d)

def f2(tablename,y,m,d):
    for year in y:
        for month in m:
            for day in d:
                try:
                    url = 'http://www.twse.com.tw/ch/trading/fund/T86/T86.php'
                    input_date=year+'/'+month+'/'+day
                    headers = {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
                    payload = {'input_date': input_date, 'select2': 'ALLBUT0999','sorting': 'by_issue'}
                    source_code = requests.post(url, headers=headers, data=payload)  # should use data instead of params
                    source_code.encoding = 'big5'
                    plain_text = source_code.text
                    soup = BeautifulSoup(plain_text, 'html.parser')
                    date=soup.find_all('thead')[0].find_all('tr')[0].find_all('th')[0].string
                    ymd = re.findall(r"\d\d\d?", date)
                    h=['年月日']
                    for th in soup.find_all('thead')[0].find_all('tr')[1].find_all('th'):
                        h.append(th.text)
                    l=[h]
                    for tr in soup.find_all('tbody')[0].find_all('tr'):
                        r = [str(int(ymd[0]) + 1911) + '/' + ymd[1] + '/' + ymd[2]]
                        for td in tr.find_all('td'):
                            r.append(td.string)
                        l.append(r)
                    df = DataFrame(l)
                    df.columns=df.ix[0,:]
                    df=df.ix[1:len(df),:]
                    df = df.replace(',', '', regex=True)
                    df['證券代號'], df['證券名稱'] = df['證券代號'].str.strip(), df['證券名稱'].str.strip()
                    df['自營商買進股數']=df['自營商買進股數(自行買賣)']+df['自營商買進股數(避險)']
                    df['自營商賣出股數'] = df['自營商賣出股數(自行買賣)'] + df['自營商賣出股數(避險)']
                    df = df[['年月日', '證券代號', '證券名稱', '外資買進股數', '外資賣出股數', '投信買進股數', '投信賣出股數', '自營商買進股數(自行買賣)',
                               '自營商賣出股數(自行買賣)', '自營商買進股數(避險)', '自營商賣出股數(避險)', '自營商買進股數', '自營商賣出股數', '三大法人買賣超股數']]
                    c.executemany('INSERT INTO `' + tablename + '` VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)', df.values.tolist())
                    conn.commit()
                    df.to_csv('C:/Users/ak66h_000/OneDrive/webscrap/tse/三大法人/三大法人/' + year + month + day + '.csv',index=False)
                    print(date)
                except Exception as e:
                    print(e)
                    pass
    print('2done')

tablename='三大法人買賣超日報(股)'
y=['103']
m=['12']
d=['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31']
f2(tablename,y,m,d)
y=['104']
m=['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
d=['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31']
f2(tablename,y,m,d)
y=['105']
m=['01', '02']
d=['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31']
f2(tablename,y,m,d)
y=['105']
m=['03']
d=['01', '02', '03', '04', '05', '06', '07']

#---read csv---
import re
import os
os.getcwd()
dir()
os.listdir()
path = 'C:/Users/ak66h_000/OneDrive/webscrap/tse/三大法人/三大法人/'
os.chdir(path)
l = os.listdir()
L = []
for i in l:
    df = read_csv(i, encoding='cp950')
    # df1 = DataFrame()
    # df = concat([df1, df], axis=1)
    L.append(df)
df1 = reduce(mymerge, L)
print(df1)
df1.to_csv('C:/Users/ak66h_000/OneDrive/webscrap/tse/三大法人/三大法人買賣超日報(股).csv',index=False)
df1=read_csv('C:/Users/ak66h_000/OneDrive/webscrap/tse/三大法人/三大法人買賣超日報(股).csv',encoding='cp950')
df1=df1[['年月日', '證券代號', '證券名稱', '外資買進股數', '外資賣出股數', '投信買進股數', '投信賣出股數', '自營商買進股數(自行買賣)', '自營商賣出股數(自行買賣)', '自營商買進股數(避險)', '自營商賣出股數(避險)', '自營商買進股數', '自營商賣出股數', '三大法人買賣超股數']]

#----create table----
tablename='三大法人買賣超日報(股)'
names=list(df1)
sql = "create table `" + tablename + "`(" + "'" + names[0] + "'"
for n in names[1:len(names)]:
    sql = sql + ',' + "'" + n + "'"
sql = sql + ',PRIMARY KEY (`年月日`,`證券代號`))'
c.execute(sql)

#---insert into sqlite---
df1=df1.drop_duplicates(subset=['年月日','證券代號'])
df1=df1.drop_duplicates()
c.executemany('INSERT INTO `' + tablename + '` VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)', df1.values.tolist())
conn.commit()

du=df1.duplicated(subset=['年月日','證券代號']).tolist()
[x for x in du if x == True]

#---insert into sqlite by name(very slow)---
tablename='三大法人買賣超日報(股)'
names=list(df1)
c = conn.cursor()
for i in range(0,len(df1)):
    sql = "insert into `" +tablename + "`(" + "'" + names[0] + "'"
    for n in names[1:len(names)]:
        sql = sql + ',' + "'" + n + "'"
    sql = sql + ')'

    sql1 = "VALUES (" + "'" + df1.values.tolist()[i][0] + "'"
    for n in df1.values.tolist()[i][1:len(df1.values.tolist()[i])]:
        sql1 = sql1 + ',' + "'" + str(n) + "'"
    sql1 = sql1 + ')'

    sql2=sql+sql1
    c.execute(sql2)
    conn.commit()
print('finish')



