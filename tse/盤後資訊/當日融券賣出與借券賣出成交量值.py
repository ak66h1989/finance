# sqlite3 can only run in console
from sqlite3 import *
conn = connect('C:\\Users\\ak66h_000\\Documents\\TEJ.sqlite3')
c = conn.cursor()

import requests
from bs4 import BeautifulSoup
from numpy import *
from pandas import *
import re
url = 'http://www.twse.com.tw/ch/trading/exchange/TWTASU/TWTASU.php'
#----create table----

payload = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36',
    'input_date': '105/02/15'}
source_code = requests.post(url, params=payload)
source_code.encoding = 'big5'
plain_text = source_code.text
print(plain_text)
soup = BeautifulSoup(plain_text, 'html.parser')
date = soup.find_all('thead')[0].find_all('tr')[0].find_all('th')[0].text

th1=soup.find_all('thead')[0].find_all('tr')[1].find_all('th')
th2=soup.find_all('thead')[0].find_all('tr')[2].find_all('th')

l = [['年月日',th1[0].string,th1[1].string+th2[0].string,th1[1].string+th2[1].string ,th1[2].string+th2[0].string,th1[2].string+th2[1].string]]
for tr in soup.find_all('tbody')[0].find_all('tr'):
    r = [date.split()[0]]
    for td in tr.find_all('td'):
        r.append(td.string)
    l.append(r)
df = DataFrame(l)
df.columns = df.ix[0, :]
df = df.ix[1:len(df), :]
df=df.replace(',','',regex=True)
df['證券代號'], df['證券名稱'] = df['證券代號'].str.strip(), df['證券名稱'].str.strip()
tablename0 =list(df)
names = list(df)
sql = "create table `" + '當日融券賣出與借券賣出成交量值(元)-presplit' + "`(" + "'" + names[0] + "'"
for n in names[1:len(names)]:
    sql = sql + ',' + "'" + n + "'"
sql = sql + ',PRIMARY KEY (`年月日`,`證券名稱`))'
c.execute(sql)

#----renew----

y=['105']
m=['02']
d=['23','22','21','20', '19', '18', '17', '16', '15','14', '13', '12', '11','10','09','08','07', '06','05','04','03', '02','01']

# y=['104','103','102','101','100','99','98','97']
# m=['12','11','10','09','08','07','06','05','04','03','02','01']
# d=['31', '30', '29','28','27','26','25','24', '23','22','21','20', '19', '18', '17', '16', '15','14', '13', '12', '11','10','09','08','07', '06','05','04','03', '02','01']
def f2(tablename,y,m,d):
    for year in y:
        for month in m:
            for day in d:
                try:
                    url = 'http://www.twse.com.tw/ch/trading/exchange/TWTASU/TWTASU.php'
                    input_date=year+'/'+month+'/'+day
                    payload = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36',
                               'input_date': input_date}
                    source_code = requests.post(url,params=payload)
                    source_code.encoding = 'big5'
                    plain_text = source_code.text
                    soup = BeautifulSoup(plain_text, 'html.parser')
                    date=soup.find_all('thead')[0].find_all('tr')[0].find_all('th')[0].text
                    ymd = re.findall(r"\d\d\d?", date)
                    th1 = soup.find_all('thead')[0].find_all('tr')[1].find_all('th')
                    th2 = soup.find_all('thead')[0].find_all('tr')[2].find_all('th')

                    l = [['年月日', th1[0].string, th1[1].string + th2[0].string, th1[1].string + th2[1].string,
                          th1[2].string + th2[0].string, th1[2].string + th2[1].string]]
                    for tr in soup.find_all('tbody')[0].find_all('tr'):
                        r = [str(int(ymd[0]) + 1911) + '/' + ymd[1] + '/' + ymd[2]]
                        for td in tr.find_all('td'):
                            r.append(td.string)
                        l.append(r)
                    df = DataFrame(l)
                    df.columns = df.ix[0, :]
                    df = df.ix[1:len(df), :]
                    df = df.replace(',', '', regex=True)
                    df['證券代號'], df['證券名稱'] = df['證券代號'].str.strip(), df['證券名稱'].str.strip()
                    print(date)

                    c.executemany('INSERT INTO `'+tablename+'` VALUES (?,?,?,?,?,?)', df.values.tolist())
                    conn.commit()
                except Exception as e:
                    print(e)
                    pass

tablename='當日融券賣出與借券賣出成交量值(元)-presplit'
y=['97', '98', '99', '100', '101', '102', '103', '104']
m=['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
d=['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31']
f2(tablename,y,m,d)
y=['105']
m=['01', '02']
d=['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31']
f2(tablename,y,m,d)
y=['105']
m=['03']
d=['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13']
f2(tablename,y,m,d)

#----update using datetime----
import datetime

tablename='當日融券賣出與借券賣出成交量值(元)'
startdate = datetime.datetime(2016, 4, 8)
delta = datetime.datetime.now() - startdate
for t in range(delta.days):
    date = startdate + datetime.timedelta(days=t + 1)
    try:
        print(date.year, date.month, date.day)
        url = 'http://www.twse.com.tw/ch/trading/exchange/TWTASU/TWTASU.php'
        if len(str(date.month)) == 1:
            input_date= str(date.year-1911)+'/'+'0'+str(date.month)+'/'+str(date.day)
        if len(str(date.month)) == 2:
            input_date= str(date.year-1911)+'/'+str(date.month)+'/'+str(date.day)
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
        payload = {'input_date': input_date}
        source_code = requests.post(url, headers=headers, data=payload)
        source_code.encoding = 'big5'
        plain_text = source_code.text
        soup = BeautifulSoup(plain_text, 'html.parser')
        title=soup.find_all('thead')[0].find_all('tr')[0].find_all('th')[0].text
        ymd = re.findall(r"\d\d\d?", title)
        th1 = soup.find_all('thead')[0].find_all('tr')[1].find_all('th')
        th2 = soup.find_all('thead')[0].find_all('tr')[2].find_all('th')

        l = [['年月日', th1[0].string, th1[1].string + th2[0].string, th1[1].string + th2[1].string,
              th1[2].string + th2[0].string, th1[2].string + th2[1].string]]
        for tr in soup.find_all('tbody')[0].find_all('tr'):
            r = [str(int(ymd[0]) + 1911) + '/' + ymd[1] + '/' + ymd[2]]
            for td in tr.find_all('td'):
                r.append(td.string)
            l.append(r)
        df = DataFrame(l)
        df.columns = df.ix[0, :]
        df = df.ix[1:len(df), :]
        df = df.replace(',', '', regex=True)
        df = df.rename(columns={'證券名稱':'證券名稱0'})
        df['證券代號']=df['證券名稱0'].str.split().str[0]
        df['證券名稱']=df['證券名稱0'].str.split().str[1]
        df=df[['年月日', '證券代號', '證券名稱', '融券賣出成交數量', '融券賣出成交金額', '借券賣出成交數量', '借券賣出成交金額']]
        df['證券代號'], df['證券名稱'] = df['證券代號'].str.strip(), df['證券名稱'].str.strip()
        print(title)

        c.executemany('INSERT INTO `'+tablename+'` VALUES (?,?,?,?,?,?,?)', df.values.tolist())
        conn.commit()
    except Exception as e:
        print(e)
        pass

print('finish')


