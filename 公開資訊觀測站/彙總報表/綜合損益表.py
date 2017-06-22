#----import----
from sqlite3 import *
conn = connect('C:\\Users\\ak66h_000\\Documents\\db\\summary.sqlite3')
c = conn.cursor()
# import psycopg2
# conn = psycopg2.connect("dbname = 公開資訊觀測站 user = postgres password = d03724008")
# cur = conn.cursor()

import requests
from bs4 import BeautifulSoup
from numpy import *
from pandas import *
from functools import *

# def mymerge(x, y):
#     m = merge(x, y, how = 'outer')
#     return m
def mymerge(x, y):
    m = merge(x, y, on = [col for col in list(x) if col in list(y)], how = 'outer')
    return m

#----test connection----
#2015
YEAR = '2015'
df1 = DataFrame()
SEASON = '2'
url = 'http://mops.twse.com.tw/mops/web/ajax_t163sb04'
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
payload = {'step':'1', 'CO_ID': CO_ID, 'SYEAR':YEAR, 'SSEASON':SEASON, 'REPORT_ID':'A'}
source_code = requests.post(url, headers = headers, data = payload) #should use data instead of params
source_code.encoding = 'big5'
plain_text = source_code.text
# print(plain_text)

# ----create table from csv----
industry = ['銀行業', '證券業', '一般業', '金控業', '保險業', '未知業']
for ind in industry:
    df1 = read_csv('C:/Users/ak66h_000/Dropbox/webscrap/公開資訊觀測站/彙總報表/綜合損益表/綜合損益表-'+ind+'.csv', encoding = 'cp950')
    names = list(df1)
    c = conn.cursor()
    sql = "create table `" + '綜合損益表-' + ind + "`(" + "'" + names[0] + "'"
    for n in names[1:len(names)]:
        sql = sql + ',' + "'" + n + "'"
    sql = sql + ')'
    c.execute(sql)
    # ----inserting data from csv----
    sql = 'INSERT INTO `' + '綜合損益表-' + ind + '` VALUES (?'
    n = [',?'] * (len(names) - 1)
    for h in n:
        sql = sql + h
    sql = sql + ')'
    c.executemany(sql, df1.values.tolist())
    conn.commit()
    print('done')
# drop table
industry = ['銀行業', '證券業', '一般業', '金控業', '保險業', '未知業']
for ind in industry:
    c = conn.cursor()
    sql = "drop table `" + '綜合損益表-' + ind + "`"
    c.execute(sql)

#---read from sqlite---
df = read_sql_query("SELECT * from `綜合損益表-一般業`", conn)
df['年'] = [x.split('/')[0] for x in df['年季']]
df['季'] = [x.split('/')[1] for x in df['年季']]
df = df[['年季', '年', '季', '公司代號', '公司名稱', '營業收入', '營業成本', '營業毛利（毛損）', '未實現銷貨（損）益', '已實現銷貨（損）益', '營業毛利（毛損）淨額', '營業費用', '其他收益及費損淨額', '營業利益（損失）', '營業外收入及支出', '稅前淨利（淨損）', '所得稅費用（利益）', '繼續營業單位本期淨利（淨損）', '停業單位損益', '合併前非屬共同控制股權損益', '本期淨利（淨損）', '其他綜合損益（淨額）', '合併前非屬共同控制股權綜合損益淨額', '本期綜合損益總額', '淨利（淨損）歸屬於母公司業主', '淨利（淨損）歸屬於共同控制下前手權益', '淨利（淨損）歸屬於非控制權益', '綜合損益總額歸屬於母公司業主', '綜合損益總額歸屬於共同控制下前手權益', '綜合損益總額歸屬於非控制權益', '基本每股盈餘（元）']]
df = df.replace('--', 'NaN')
for i in ['營業收入', '營業成本', '營業毛利（毛損）', '未實現銷貨（損）益', '已實現銷貨（損）益', '營業毛利（毛損）淨額', '營業費用', '其他收益及費損淨額', '營業利益（損失）', '營業外收入及支出', '稅前淨利（淨損）', '所得稅費用（利益）', '繼續營業單位本期淨利（淨損）', '停業單位損益', '合併前非屬共同控制股權損益', '本期淨利（淨損）', '其他綜合損益（淨額）', '合併前非屬共同控制股權綜合損益淨額', '本期綜合損益總額', '淨利（淨損）歸屬於母公司業主', '淨利（淨損）歸屬於共同控制下前手權益', '淨利（淨損）歸屬於非控制權益', '綜合損益總額歸屬於母公司業主', '綜合損益總額歸屬於共同控制下前手權益', '綜合損益總額歸屬於非控制權益', '基本每股盈餘（元）']:
    try:
        if df[i].dtypes is dtype('O'):
            df[[i]] = df[[i]].astype(float)
    except Exception as e:
        print(i)
        print(e)
df1 = df.copy()
for i in ['營業收入', '營業成本', '營業毛利（毛損）', '未實現銷貨（損）益', '已實現銷貨（損）益', '營業毛利（毛損）淨額', '營業費用', '其他收益及費損淨額', '營業利益（損失）', '營業外收入及支出', '稅前淨利（淨損）', '所得稅費用（利益）', '繼續營業單位本期淨利（淨損）', '停業單位損益', '合併前非屬共同控制股權損益', '本期淨利（淨損）', '其他綜合損益（淨額）', '合併前非屬共同控制股權綜合損益淨額', '本期綜合損益總額', '淨利（淨損）歸屬於母公司業主', '淨利（淨損）歸屬於共同控制下前手權益', '淨利（淨損）歸屬於非控制權益', '綜合損益總額歸屬於母公司業主', '綜合損益總額歸屬於共同控制下前手權益', '綜合損益總額歸屬於非控制權益', '基本每股盈餘（元）']:
    try:
        df1[i] = df1.groupby(['公司代號', '年'])[i].apply(lambda x: x-x.shift(-1))
    except Exception as e:
        print(i)
        print(e)
df1['grow'] = df.groupby(['公司代號'])['本期綜合損益總額'].pct_change(-1)
df1[df1['季']  =  =  '01'] = df[df['季']  =  =  '01']
df1 = df1.sort_values(['公司代號','年季'],ascending = [True,True])
df1['grow.ma'] = df1.groupby(['公司代號'])['grow'].apply(rolling_mean,10)
df1 = df1[['年季', '公司代號', '公司名稱', '營業收入', '營業成本', '營業毛利（毛損）', '未實現銷貨（損）益', '已實現銷貨（損）益', '營業毛利（毛損）淨額', '營業費用', '其他收益及費損淨額', '營業利益（損失）', '營業外收入及支出', '稅前淨利（淨損）', '所得稅費用（利益）', '繼續營業單位本期淨利（淨損）', '停業單位損益', '合併前非屬共同控制股權損益', '本期淨利（淨損）', '其他綜合損益（淨額）', '合併前非屬共同控制股權綜合損益淨額', '本期綜合損益總額', '淨利（淨損）歸屬於母公司業主', '淨利（淨損）歸屬於共同控制下前手權益', '淨利（淨損）歸屬於非控制權益', '綜合損益總額歸屬於母公司業主', '綜合損益總額歸屬於共同控制下前手權益', '綜合損益總額歸屬於非控制權益', '基本每股盈餘（元）']]
df1 = df1.sort_values(['公司代號','年季'],ascending = [True,False])
print(df1)
df1.to_csv('C:/Users/ak66h_000/Dropbox/webscrap/公開資訊觀測站/彙總報表/季/綜合損益表/綜合損益表(季)-一般業.csv')

#---- rename table ----
report = '綜合損益表-'
industry = ['銀行業', '證券業', '一般業', '金控業', '保險業', '未知業']
for ind in industry:
    sql = 'ALTER TABLE `'+ report + ind +'` RENAME TO`' + report + ind +'0`'
    c.execute(sql)
# ----create table from csv----
dic = {1: "綜合損益表-銀行業", 2: "綜合損益表-證券業", 3: "綜合損益表-一般業", 4: "綜合損益表-金控業", 5: "綜合損益表-保險業", 6: "綜合損益表-未知業"}
for key in dic.keys():
    df = read_sql_query("SELECT * from `" + dic[key] + "0`", conn)
    path = 'C:/Users/ak66h_000/Dropbox/webscrap/公開資訊觀測站/彙總報表/綜合損益表/'+dic[key]+'/'
    os.chdir(path)
    l = os.listdir()
    L = []
    for i in l:
        df1 = read_csv(i, encoding = 'cp950')
        df1[['年', '季', '公司代號']] = df1[['年', '季', '公司代號']].astype(str).replace('\.0', '', regex = True)
        df1['季'] = '0'+df1['季']
        df1['公司代號'], df1['公司名稱'] = df1['公司代號'].str.strip(), df1['公司名稱'].str.strip()
        L.append(df1)
    df1 = reduce(mymerge, L)
    df2 = mymerge(df, df1).sort_values(['年', '季', '公司代號']).drop_duplicates(['年', '季', '公司代號'])
    # ----create table----
    names = list(df2)
    c = conn.cursor()
    sql = "create table `" + dic[key] + "`(" + "'" + names[0] + "'"
    for n in names[1:len(names)]:
        sql = sql + ',' + "'" + n + "'"
    sql = sql + ', PRIMARY KEY (`年`, `季`, `公司代號`))'
    c.execute(sql)
    # ----inserting data----
    sql = 'INSERT INTO `' + dic[key] + '` VALUES (?'
    n = [',?'] * (len(names) - 1)
    for h in n:
        sql = sql + h
    sql = sql + ')'
    c.executemany(sql, df2.values.tolist())
    conn.commit()
    print('done')
# ----drop table----
for ind in industry:
    c = conn.cursor()
    sql = "drop table `" + report + ind + "0`"
    c.execute(sql)

# ----reorgnize table----
for key in dic:
    df = read_sql_query("SELECT * from `%s`"%dic[key], conn)
    df['年'], df['季'], df['公司代號'] =  df['年'].astype(int), df['季'].astype(int), df['公司代號'].astype(str)
    df = df.drop_duplicates(subset = ['年','季', '公司代號']).sort_values(['年','季', '公司代號'])
    sql = 'ALTER TABLE `%s` RENAME TO `%s0`'%(dic[key], dic[key])
    c.execute(sql)
    sql = 'create table `%s` (`%s`, PRIMARY KEY (%s))'%(dic[key], '`,`'.join(list(df)), '`年`, `季`, `公司代號`')
    c.execute(sql)
    sql = 'insert into `%s`(`%s`) values(%s)'%(dic[key], '`,`'.join(list(df)), ','.join('?'*len(list(df))))
    c.executemany(sql, df.values.tolist())
    conn.commit()
    sql = "drop table `%s0`"%dic[key]
    c.execute(sql)
print('finish')

#----update----

dic = {1: "綜合損益表-銀行業", 2: "綜合損益表-證券業", 3: "綜合損益表-一般業", 4: "綜合損益表-金控業", 5: "綜合損益表-保險業", 6: "綜合損益表-其他業"}
# dic = {1:"綜合損益表_銀行業",2:"綜合損益表_證券業",3:"綜合損益表_一般業",4:"綜合損益表_金控業",5:"綜合損益表_保險業",6:"綜合損益表_未知業"}
error = []
for key in dic.keys():
    L = []
    for YEAR in ['105']:
        for SEASON in ['03']:
            try:
                y = str(int(YEAR)+1911)
                print(y, SEASON, dic[key])
                url = 'http://mops.twse.com.tw/mops/web/ajax_t163sb04'
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:45.0) Gecko/20100101 Firefox/45.0'}
                payload = {'encodeURIComponent': '1', 'step': '1', 'firstin': '1', 'off': '1', 'TYPEK': 'sii', 'year': YEAR, 'season': SEASON}
                source_code = requests.post(url, headers = headers, data = payload)
                source_code.encoding = 'utf-8'
                plain_text = source_code.text
                soup = BeautifulSoup(plain_text, 'html.parser')
                h = ['年', '季']
                for th in soup.find_all('table')[key].find_all('tr')[0].find_all('th'):
                    h.append(th.text)
                soup.find_all('table')[key].find_all('tr')[1].find_all('td')
                row = len(soup.find_all('table')[key].find_all('tr'))
                td = soup.find_all('table')[key].find_all('tr')[1].find_all('td')
                l = [h]
                for i in range(1, row):
                    r = [y, SEASON]
                    for j in range(0, len(h)-2):
                        td = soup.find_all('table')[key].find_all('tr')[i].find_all('td')[j].text
                        r.append(td)
                    l.append(r)
                df = DataFrame(l)
                df.columns = df.ix[0, :]
                df = df.ix[1:len(df), :]
                df = df.replace(',', '', regex = True)
                df['公司代號'], df['公司名稱'] = df['公司代號'].str.strip(), df['公司名稱'].str.strip()
                df.to_csv('C:/Users/ak66h_000/Dropbox/webscrap/公開資訊觀測站/彙總報表/綜合損益表/'+dic[key]+'/'+dic[key]+y+SEASON+'.csv', index = False)
                print(df)
                L.append(df) # do not use L = L.append(df)!!
            except Exception as e:
                print(e)
                error.append([y, SEASON, dic[key]])
                pass
for key in dic:
    path = 'C:/Users/ak66h_000/Dropbox/webscrap/公開資訊觀測站/彙總報表/綜合損益表/%s/'%dic[key]
    os.chdir(path)
    L = os.listdir()
    try:
        df = read_csv(L[-1], encoding = 'cp950')
        df['年'], df['季'], df['公司代號'] =  df['年'].astype(int), df['季'].astype(int), df['公司代號'].astype(str)
        sql = 'insert into `%s`(`%s`) values(%s)'%(dic[key], '`,`'.join(list(df)), ','.join('?'*len(list(df))))
        c.executemany(sql, df.values.tolist())
        conn.commit()
    except Exception as e: print(e); pass









