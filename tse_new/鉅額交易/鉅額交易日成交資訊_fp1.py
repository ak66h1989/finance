import pandas as pd
import datetime, json, sys
sys.path.append('C:/Users/user/Dropbox/program/mypackage_py')
import crawler_fp1 as crawler
import functools

###----鉅額交易日成交資訊----

def urlFunc(input_date, type):
    url = 'http://www.twse.com.tw/block/BFIAUU?response=json&date={}&selectType={}'.format(input_date, type)
    return url

def getPlainText(url):
    return crawler.plainText_get(url)

# -- 1 company in 1 day may have more than 1 transaction --
def f1(df):
    df.第幾筆 = list(range(1,len(df.第幾筆)+1))
    return df

lastdate = crawler.lastDate('鉅額交易日成交資訊')
input_date_func = functools.partial(crawler.inputDate, lastdate=lastdate)

def crawler_hugeDeal(t):
    input_date = input_date_func(t)
    url = urlFunc(input_date, 'S')
    d = json.loads(getPlainText(url))
    data = d['data']
    fields = d['fields']
    date = d['date'][0:4] + '-' + d['date'][4:6] + '-' + d['date'][6:]
    df = pd.DataFrame(data, columns=fields).replace(',', '', regex=True).replace('--', 'NaN').replace('', 'NaN')
    df.insert(0, '年月日', date)
    df.insert(len(list(df)), '第幾筆', 1)
    df['年月日'] = pd.to_datetime(df['年月日']).astype(str)
    intColumns = ['第幾筆']
    floatColumns = []
    for col in ['成交價', '成交股數', '成交金額', '成交量']:
        if col in list(df):
            floatColumns.append(col)
    df[floatColumns] = df[floatColumns].astype(float)
    df[intColumns] = df[intColumns].astype(int)
    df = df.groupby(['年月日', '證券代號']).apply(f1)
    return df


saver = functools.partial(crawler.saveToSqlite, tablename='鉅額交易日成交資訊')
# crawAndSave = functools.partial(crawler.crawAndSaveFunc, crawlerFunc=crawler_hugeDeal, saverFunc=saver)
max = crawler.timeDelta(lastdate).days
# g=crawler.looper(crawAndSave, 10)
g=crawler.looper(crawler_hugeDeal, saver, 10)
next(g)

# crawAndSave(0)

def pr(t):
    print(t)

def looper(crawAndSaveFunc, max):
    for t in range(max+1):
        try:
            yield t, crawAndSaveFunc(t)
        except Exception as e:
            print(t, e)
            pass


g=crawler.looper(pr, 10)
next(g)
