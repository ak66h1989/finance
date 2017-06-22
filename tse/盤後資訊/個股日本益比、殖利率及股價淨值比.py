## ------ pe is '-' when pe < 0 ------

# sqlite3 can only run in console
from sqlite3 import *
conn = connect('C:\\Users\\ak66h_000\\Documents\\TEJ.sqlite3')
c = conn.cursor()

import requests
from bs4 import BeautifulSoup
from numpy import *
from pandas import *
from pandas import *
import re
url = 'http://www.twse.com.tw/ch/trading/exchange/BWIBBU/BWIBBU_d.php'

#----create table----

payload = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36',
    'input_date': '105/02/03', 'select2': 'ALL', 'order': 'STKNO'}
source_code = requests.post(url, params=payload)
source_code.encoding = 'big5'
plain_text = source_code.text
soup = BeautifulSoup(plain_text, 'html.parser')
date = soup.find_all('thead')[0].find_all('tr')[0].find_all('th')[0].string

h = ['年月日']
for tr in soup.find_all('thead')[0].find_all('tr')[1]:
    h.append(tr.text)
l = [h]
df = DataFrame(l)
df.columns = df.ix[0, :]
df = df.ix[1:len(df), :]
tablename0 =list(df)
names = list(df)
sql = "create table `" + '個股日本益比、殖利率及股價淨值比' + "`(" + "'" + names[0] + "'"
for n in names[1:len(names)]:
    sql = sql + ',' + "'" + n + "'"
sql = sql + ',PRIMARY KEY (`年月日`,`證券代號`))'
c.execute(sql)

#----update using datetime----
import datetime

tablename='個股日本益比、殖利率及股價淨值比'
startdate = datetime.datetime(2016, 4, 8)
delta = datetime.datetime.now() - startdate
for t in range(delta.days):
    date = startdate + datetime.timedelta(days=t + 1)
    try:
        url = 'http://www.twse.com.tw/ch/trading/exchange/BWIBBU/BWIBBU_d.php'
        month, day = date.month, date.day
        if len(str(month)) == 1:
            month='0'+str(month)
        if len(str(day)) == 1:
            day='0'+str(day)
        input_date = str(date.year - 1911) + '/'  + str(month) + '/' + str(day)
        print(date.year, date.month, date.day, input_date)
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
        payload = {'input_date': input_date, 'select2': 'ALL', 'order': 'STKNO'}
        source_code = requests.post(url, headers=headers, data=payload)
        source_code.encoding = 'big5'
        plain_text = source_code.text
        soup = BeautifulSoup(plain_text, 'html.parser')
        title=soup.find_all('thead')[0].find_all('tr')[0].find_all('th')[0].string
        ymd = re.findall(r"\d\d\d?", title)
        h=['年月日']
        for tr in soup.find_all('thead')[0].find_all('tr')[1]:
            h.append(tr.text)
        l=[h]
        for tr in soup.find_all('tbody')[0].find_all('tr'):
            r = [str(int(ymd[0]) + 1911) + '/' + ymd[1] + '/' + ymd[2]]
            for td in tr.find_all('td'):
                r.append(td.string)
            l.append(r)
        df = DataFrame(l)
        df.columns=df.ix[0,:]
        df=df.ix[1:len(df),:]
        df['證券代號'], df['證券名稱'] = df['證券代號'].str.strip(), df['證券名稱'].str.strip()
        print(title)

        c.executemany('INSERT INTO `'+tablename+'` VALUES (?,?,?,?,?,?)', df.values.tolist())
        conn.commit()
    except Exception as e:
        print(e)
        pass



