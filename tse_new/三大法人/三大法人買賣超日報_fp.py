##----- pe is '0.00' when pe < 0 -----

import pandas as pd
import datetime, json, sys
sys.path.append('C:/Users/user/Dropbox/program/mypackage_py')
import crawler_fp as crawler

###----三大法人買賣超日報----

def getPlainText(lastdate, t, type):
    input_date = crawler.inputDate(lastdate, t)
    url = 'http://www.twse.com.tw/fund/T86?response=json&date={}&selectType={}'.format(input_date, type)
    return crawler.plainText_get(url)

def institutional(lastdate, t, type):
    d = json.loads(getPlainText(lastdate, t, type))
    data = d['data']
    fields = d['fields']
    fields = [s.replace('</br>', '') for s in fields]
    date = d['date'][0:4] + '-' + d['date'][4:6] + '-' + d['date'][6:]
    df = pd.DataFrame(data, columns=fields).replace(',', '', regex=True).replace('--', 'NaN').replace('</br>', '', regex=True)
    df.insert(0, '年月日', date)
    df['年月日'] = pd.to_datetime(df['年月日']).astype(str)
    df['證券名稱'] = df['證券名稱'].str.strip()
    if '自營商買進股數' in list(df):
        floatColumns = ['外資買進股數', '外資賣出股數', '外資買賣超股數', '投信買進股數', '投信賣出股數', '投信買賣超股數', '自營商買賣超股數', '自營商買進股數', '自營商賣出股數', '三大法人買賣超股數']
        df[floatColumns] = df[floatColumns].astype(float)
    else:
        floatColumns = ['外資買進股數', '外資賣出股數', '外資買賣超股數', '投信買進股數', '投信賣出股數', '投信買賣超股數', '自營商買進股數(自行買賣)', '自營商賣出股數(自行買賣)', '自營商買賣超股數(自行買賣)', '自營商買進股數(避險)', '自營商賣出股數(避險)', '自營商買賣超股數(避險)', '三大法人買賣超股數']
        df[floatColumns] = df[floatColumns].astype(float)
    return df

g = crawler.crawToSqlite(institutional, '三大法人買賣超日報', 'ALL')
# next(g)
for _ in g: pass