# sqlite3 can only run in console
from sqlite3 import *
conn = connect('C:\\Users\\ak66h_000\\Documents\\TEJ.sqlite3')
c = conn.cursor()

import requests
from bs4 import BeautifulSoup
from numpy import *
from pandas import *

#----get unique id----
url = 'http://www.twse.com.tw/ch/trading/exchange/BWIBBU/BWIBBU_d.php'
payload = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36',
    'input_date': '105/02/15', 'select2': 'ALL', 'order': 'STKNO'}
source_code = requests.post(url, params=payload)
source_code.encoding = 'big5'
plain_text = source_code.text
print(plain_text)
soup = BeautifulSoup(plain_text, 'html.parser')
date = soup.find_all('thead')[0].find_all('tr')[0].find_all('th')[0].string

h = ['年月日']
for tr in soup.find_all('thead')[0].find_all('tr')[1]:
    h.append(tr.text)
l = [h]
for tr in soup.find_all('tbody')[0].find_all('tr'):
    r = [date.split()[0] + date.split()[0]]
    for td in tr.find_all('td'):
        r.append(td.string)
    l.append(r)
df = DataFrame(l)
df.columns = df.ix[0, :]
df = df.ix[1:len(df), :]

id=df.ix[:, 1].unique().tolist()
for u in id:
    print(u)

# #----create table----
year='2016'
month='02'
u='5522'
url = 'http://www.twse.com.tw/ch/trading/exchange/STOCK_DAY/genpage/Report' + year + month + '/' + year + month + '_F3_1_8_' + u + '.php?STK_NO=' + u + '&myear=' + year + '&mmon=' + month
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
source_code = requests.get(url,headers=headers)
source_code.encoding = 'big5'
plain_text = source_code.text
print(plain_text)
soup = BeautifulSoup(plain_text, 'html.parser')
date = soup.find_all('table')[7].find_all('tr')[0].text

h = ['證券代號','證券名稱']
for td in soup.find_all('table')[7].find_all('tr')[1].find_all('td'):
    h.append(td.text)
l = [h]
table=soup.find_all('table')[7].find_all('tr')
for tr in table[2:len(table)-1]:
    r = [date.split()[1] , date.split()[2]]
    for td in tr.find_all('td'):
        r.append(td.string)
    l.append(r)
df = DataFrame(l)
df.columns = df.ix[0, :]
df = df.ix[1:len(df), :]
names = list(df)
sql = "create table `" + '個股日成交資訊' + "`(" + "'" + names[0] + "'"
for n in names[1:len(names)]:
    sql = sql + ',' + "'" + n + "'"
sql = sql + ')'
c.execute(sql)
#
# #----test inserting data----
# c.executemany('INSERT INTO `個股日成交資訊` VALUES (?,?,?,?,?,?,?,?,?,?,?)', df.values.tolist())
# conn.commit()

#----renew----
# y=['2015','2014','2013','2012','2011','2010','2009','2008','2007','2006','2005','2004','2003','2002','2001','2000','1999','1998','1997','1996','1995','1994','1993']
# m=['12','11','10','09','08','07','06','05','04','03','02','01']
# print(id)
y=['2016']
m=['02']
for year in y:
    for month in m:
        for u in id:
            try:
                url='http://www.twse.com.tw/ch/trading/exchange/STOCK_DAY/genpage/Report' + year + month + '/' + year + month + '_F3_1_8_' + u + '.php?STK_NO=' + u + '&myear=' + year + '&mmon=' + month
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'
                    }
                source_code = requests.get(url, headers=headers)
                source_code.encoding = 'big5'
                plain_text = source_code.text
                soup = BeautifulSoup(plain_text, 'html.parser')
                date = soup.find_all('table')[7].find_all('tr')[0].find_all('div')[0].string

                h = ['證券代號','證券名稱']
                for td in soup.find_all('table')[7].find_all('tr')[1].find_all('td'):
                    h.append(td.text)
                l = [h]
                table=soup.find_all('table')[7].find_all('tr')
                for tr in table[2:len(table)-1]:
                    r = [date.split()[1] , date.split()[2]]
                    for td in tr.find_all('td'):
                        r.append(td.string)
                    l.append(r)
                df = DataFrame(l)
                df.columns = df.ix[0, :]
                df = df.ix[1:len(df), :]
                df = df.replace(',', '', regex=True)
                print(date)

                c.executemany('INSERT INTO `個股日成交資訊` VALUES (?,?,?,?,?,?,?,?,?,?,?)', df.values.tolist())
                conn.commit()
            except Exception as e:
                print(e)
                pass

#---read from sqlite---
df1 = read_sql_query("SELECT * FROM 個股日成交資訊", conn)
df1['日期'] = df1['日期'].replace('105', '2016', regex=True)
df1['日期'] = df1['日期'].replace('104', '2015', regex=True)
df1['日期'] = df1['日期'].replace('103', '2014', regex=True)
df1['日期'] = df1['日期'].replace('102', '2013', regex=True)
df1['日期'] = df1['日期'].replace('101', '2012', regex=True)
df1['日期'] = df1['日期'].replace('100', '2011', regex=True)
df1['日期'] = df1['日期'].replace('99', '2010', regex=True)
df1['日期'] = df1['日期'].replace('98', '2009', regex=True)
df1['日期'] = df1['日期'].replace('97', '2008', regex=True)
df1['日期'] = df1['日期'].replace('96', '2007', regex=True)
df1['日期'] = df1['日期'].replace('95', '2006', regex=True)
df1['日期'] = df1['日期'].replace('94', '2005', regex=True)
df1['日期'] = df1['日期'].replace('93', '2004', regex=True)
df1['日期'] = df1['日期'].replace('92', '2003', regex=True)
df1['日期'] = df1['日期'].replace('91', '2002', regex=True)
df1['日期'] = df1['日期'].replace('90', '2001', regex=True)
df1['日期'] = df1['日期'].replace('89', '2000', regex=True)
df1['日期'] = df1['日期'].replace('88', '1999', regex=True)
df1['日期'] = df1['日期'].replace('87', '1998', regex=True)
df1['日期'] = df1['日期'].replace('86', '1997', regex=True)
df1['日期'] = df1['日期'].replace('85', '1996', regex=True)
df1['日期'] = df1['日期'].replace('84', '1995', regex=True)
df1['日期'] = df1['日期'].replace('83', '1994', regex=True)
df1['日期'] = df1['日期'].replace('82', '1993', regex=True)
df1=df1.sort_values(['日期','證券代號'],ascending=[False,True])
# ----create table----
names = list(df1)
conn = connect('C:\\Users\\ak66h_000\\Documents\\TEJ.sqlite3')
c = conn.cursor()
sql = "create table `" + '個股日成交資訊' + "`(" + "'" + names[0] + "'"
for n in names[1:len(names)]:
    sql = sql + ',' + "'" + n + "'"
sql = sql + ')'
c.execute(sql)

#---read from sqlite---
df = read_sql_query("SELECT * from `個股日成交資訊`", conn)
df.duplicated(['證券代號','日期'])
df=df.drop_duplicates(['證券代號','日期'])
c.executemany('INSERT INTO `個股日成交資訊2` VALUES (?,?,?,?,?,?,?,?,?,?,?)', df.values.tolist())
conn.commit()

