#----import----

from sqlite3 import *
conn = connect('C:\\Users\\ak66h_000\\Documents\\db\\summary.sqlite3')
c = conn.cursor()

import requests
from bs4 import BeautifulSoup
from numpy import *
from pandas import *
from functools import *

# def mymerge(x, y):
#     m = merge(x, y, how='outer')
#     return m
def mymerge(x, y):
    m = merge(x, y, on=[col for col in list(x) if col in list(y)], how='outer')
    return m

# create table from csv
import os
import re
os.getcwd()
dir()
os.listdir()
industry=['銀行業', '證券業', '一般業', '金控業', '保險業', '其他業']
for ind in industry:
    path = 'C:/Users/ak66h_000/Dropbox/webscrap/公開資訊觀測站/彙總報表/資產負債表/彙總報表/'+ind
    os.chdir(path)
    l = os.listdir()
    L = []
    for i in l:
        df = read_csv(i, encoding='cp950')
        # t = re.findall(r'\d', i)
        # t = t[0] + t[1] + t[2] + t[3] + '/' + t[4] + t[5]
        # d = {'年季': repeat(t, len(df))}
        # df1 = DataFrame(d)
        # df = concat([df1, df], axis=1)
        df['年'] = df['年季'].str.split('/').str[0]
        df['季'] = df['年季'].str.split('/').str[1]
        col = [x for x in list(df) if x not in ['年季', '年', '季']]
        df = df[['年', '季']+col]
        df['公司代號'] = df['公司代號'].astype(str).replace('\.0', '', regex=True)
        df['公司代號'], df['公司名稱'] = df['公司代號'].str.strip(), df['公司名稱'].str.strip()
        L.append(df)
    df1 = reduce(mymerge, L)
    df1 = df1.sort_values(['年', '季', '公司代號'])
    df1.to_csv('C:/Users/ak66h_000/Dropbox/webscrap/公開資訊觀測站/彙總報表/資產負債表/彙總報表/資產負債表-'+ind+'.csv',index=False)
    # ----create table----
    names = list(df1)
    c = conn.cursor()
    sql = "create table `" + '資產負債表-'+ind + "`(" + "'" + names[0] + "'"
    for n in names[1:len(names)]:
        sql = sql + ',' + "'" + n + "'"
    sql = sql + ', PRIMARY KEY (`年`, `季`, `公司代號`))'
    c.execute(sql)
    # ----inserting data----
    sql = 'INSERT INTO `' + '資產負債表-'+ind + '` VALUES (?'
    n = [',?'] * (len(names) - 1)
    for h in n:
        sql = sql + h
    sql = sql + ')'
    c.executemany(sql, df1.values.tolist())
    conn.commit()
    print('done')

# drop table
for ind in industry:
    c = conn.cursor()
    sql = "drop table `" + '資產負債表-' + ind + "0`"
    c.execute(sql)

###  balance sheet is weird, can't find <tbody> and tr[1], tr[2],...

#----test connection----
#2015
YEAR='104'
SEASON='04'
key=1
url = 'http://mops.twse.com.tw/mops/web/ajax_t163sb05'
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36'}
payload = {'encodeURIComponent': '1', 'step': '1', 'firstin': '1', 'off': '1', 'TYPEK': 'sii',
           'year': YEAR, 'season': SEASON}
source_code = requests.post(url, headers=headers, data=payload)
source_code.encoding = 'utf-8'
plain_text = source_code.text
print(plain_text)
soup = BeautifulSoup(plain_text, 'html.parser')
h = ['年季']
for th in soup.find_all('table')[key].find_all('tr')[0].find_all('th'):
    h.append(th.text)

row=len(soup.find_all('tr',{'class':"even"}))
l=[]
for i in range(0,row):
    r=[]
    for td in soup.find_all('tr',{'class':"even"})[i].find_all('td'):
        r.append(td.text)
    l.append(r)

tr=[x for x in l if len(x)==53]
L=[h]
for i in range(0,len(tr)):
    r=[YEAR + '/' + SEASON]+tr[i]
    L.append(r)
df = DataFrame(L)
df.columns = df.ix[0, :]
df = df.ix[1:len(df), :]
df = df.replace(',', '', regex=True)
print(df)

#----main----
# even table
dic={1:[53,"資產負債表-銀行業"],3:[22,"資產負債表-一般業"],5:[47,"資產負債表-保險業"]}
# dic={3:[22,"資產負債表-一般業"],5:[47,"資產負債表-保險業"]}
for key in dic.keys():
    L = []
    for YEAR in ['105']:
        for SEASON in ['03']:
            try:
                y = str(int(YEAR)+1911)
                print(y, SEASON, dic[key])
                url = 'http://mops.twse.com.tw/mops/web/ajax_t163sb05'
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36'}
                payload = {'encodeURIComponent': '1', 'step': '1', 'firstin': '1', 'off': '1', 'TYPEK': 'sii',
                           'year': YEAR, 'season': SEASON}
                source_code = requests.post(url, headers=headers, data=payload)
                source_code.encoding = 'utf-8'
                plain_text = source_code.text
                soup = BeautifulSoup(plain_text, 'html.parser')
                h = ['年', '季']
                for th in soup.find_all('table')[key].find_all('tr')[0].find_all('th'):
                    h.append(th.text)
                row=len(soup.find_all('tr',{'class':"even"}))
                l=[]
                for i in range(0,row):
                    r=[]
                    for td in soup.find_all('tr',{'class':"even"})[i].find_all('td'):
                        r.append(td.text)
                    l.append(r)

                tr=[x for x in l if len(x)==dic[key][0]]
                l1=[h]
                for i in range(0,len(tr)):
                    r=[y, SEASON]+tr[i]
                    l1.append(r)
                df = DataFrame(l1)
                df.columns = df.ix[0, :]
                df = df.ix[1:len(df), :]
                df = df.replace(',', '', regex=True)
                df.to_csv('C:/Users/ak66h_000/Dropbox/webscrap/公開資訊觀測站/彙總報表/資產負債表/'+ dic[key][1] + '/' + dic[key][1] + y + SEASON + '.csv',index=False)
                print(df)
                L.append(df)
            except Exception as e:
                print(e)
                pass

# odd table
dic = {2: [22, "資產負債表-證券業"], 4: [54,"資產負債表-金控業"], 6: [21,"資產負債表-其他業"]}
for key in dic.keys():
    L = []
    for YEAR in ['105']:
        for SEASON in ['03']:
            try:
                y = str(int(YEAR)+1911)
                print(y, SEASON, dic[key])
                url = 'http://mops.twse.com.tw/mops/web/ajax_t163sb05'
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36'}
                payload = {'encodeURIComponent': '1', 'step': '1', 'firstin': '1', 'off': '1', 'TYPEK': 'sii',
                           'year': YEAR, 'season': SEASON}
                source_code = requests.post(url, headers=headers, data=payload)
                source_code.encoding = 'utf-8'
                plain_text = source_code.text
                soup = BeautifulSoup(plain_text, 'html.parser')
                h = ['年', '季']
                for th in soup.find_all('table')[key].find_all('tr')[0].find_all('th'):
                    h.append(th.text)
                row=len(soup.find_all('tr',{'class':"odd"}))
                l=[]
                for i in range(0,row):
                    r=[]
                    for td in soup.find_all('tr',{'class':"odd"})[i].find_all('td'):
                        r.append(td.text)
                    l.append(r)

                tr=[x for x in l if len(x)==dic[key][0]]
                l1=[h]
                for i in range(0,len(tr)):
                    r=[y, SEASON]+tr[i]
                    l1.append(r)
                df = DataFrame(l1)
                df.columns = df.ix[0, :]
                df = df.ix[1:len(df), :]
                df = df.replace(',', '', regex=True)
                df['公司代號'], df['公司名稱'] = df['公司代號'].str.strip(), df['公司名稱'].str.strip()
                df.to_csv('C:/Users/ak66h_000/Dropbox/webscrap/公開資訊觀測站/彙總報表/資產負債表/'+ dic[key][1]+'/'+ dic[key][1] + y + SEASON + '.csv',index=False)
                print(df)
                L.append(df)
            except Exception as e:
                print(e)
                pass

# ---- update table ----
dic = {1: "資產負債表-銀行業", 2: "資產負債表-證券業", 3: "資產負債表-一般業", 4: "資產負債表-金控業", 5: "資產負債表-保險業", 6: "資產負債表-其他業"}
for key in dic:
    try:
        path='C:/Users/ak66h_000/Dropbox/webscrap/公開資訊觀測站/彙總報表/資產負債表/{}/'.format(dic[key])
        os.chdir(path)
        L=os.listdir()
        df = read_csv(L[-1], encoding='cp950')
        df.年=df.年.astype(int)
        df.季=df.季.astype(int)
        df.公司代號=df.公司代號.astype(str)
        sql='insert into `{}`(`{}`) values({})'.format(dic[key], '`,`'.join(list(df)), ','.join('?'*len(list(df))))
        c.executemany(sql, df.values.tolist())
        conn.commit()
    except Exception as e:
        print(e)
        pass

# conn = connect('C:\\Users\\ak66h_000\\Documents\\db\\summary.sqlite3')
# c = conn.cursor()
# for key in dic:
#     df = read_sql_query("SELECT * from `{}`".format(dic[key]), conn)
#     df.年 = df.年.astype(int)
#     df.季 = df.季.astype(int)
#     df.公司代號 = df.公司代號.astype(str)
#     sql = 'ALTER TABLE `{}` RENAME TO `{}0`'.format(dic[key], dic[key])
#     c.execute(sql)
#     sql = 'create table `{}` (`{}`, PRIMARY KEY ({}))'.format(dic[key], '`,`'.join(list(df)), '`年`, `季`, `公司代號`')
#     c.execute(sql)
#     sql = 'insert into `{}`(`{}`) values({})'.format(dic[key], '`,`'.join(list(df)), ','.join('?'*len(list(df))))
#     c.executemany(sql, df.values.tolist())
#     conn.commit()
#     sql = "drop table `{}0`".format(dic[key])
# print('finish')
# for key in dic:
#     sql = "drop table `{}0`".format(dic[key])
#     c.execute(sql)





