import pandas as pd
import datetime, json, sys
sys.path.append('C:/Users/user/Dropbox/program/mypackage_py')
import crawler_fp as crawler

###----鉅額交易日成交資訊----

def getPlainText(lastdate, t, type):
    input_date = crawler.inputDate(lastdate, t)
    url = 'http://www.twse.com.tw/block/BFIAUU?response=json&date={}&selectType={}'.format(input_date, type)
    return crawler.plainText_get(url)

# -- 1 company in 1 day may have more than 1 transaction --
def f1(df):
    df.第幾筆 = list(range(1,len(df.第幾筆)+1))
    return df

def hugeDeal(lastdate, t, type):
    d = json.loads(getPlainText(lastdate, t, type))
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

g = crawler.crawToSqlite(hugeDeal, '鉅額交易日成交資訊', 'S')
# next(g)
for _ in g: pass




