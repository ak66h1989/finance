##----- pe is '0.00' when pe < 0 -----

import pandas as pd
import datetime, json, sys
sys.path.append('C:/Users/user/Dropbox/program/mypackage_py')
import crawler_fp as crawler

def getPlainText(lastdate, t):
    input_date = crawler.inputDate(lastdate, t)
    url = 'http://www.twse.com.tw/exchangeReport/TWTASU?response=json&date={}'.format(input_date)
    return crawler.plainText_get(url)

def margin(lastdate, t):
    d = json.loads(getPlainText(lastdate, t))
    data = d['data']
    fields = d['fields']
    date = d['date'][0:4] + '-' + d['date'][4:6] + '-' + d['date'][6:]
    df = pd.DataFrame(data, columns=fields).replace(',', '', regex=True).replace('--', 'NaN').replace('-', 'NaN')
    df = df[df.證券名稱 != '合計']
    df.insert(0, '證券代號', df['證券名稱'].str.split().str[0].str.strip())
    df['證券名稱'] = df['證券名稱'].str.split().str[1].str.strip()
    df.insert(0, '年月日', date)
    df['年月日'] = pd.to_datetime(df['年月日']).astype(str)
    floatColumns = ['融券賣出成交金額', '借券賣出成交金額']
    df[floatColumns] = df[floatColumns].astype(float)
    intColumns = ['融券賣出成交數量', '借券賣出成交數量']
    df[intColumns] = df[intColumns].astype(int)
    return df

g = crawler.crawToSqlite(margin, '當日融券賣出與借券賣出成交量值(元)')
# next(g)
for _ in g: pass
