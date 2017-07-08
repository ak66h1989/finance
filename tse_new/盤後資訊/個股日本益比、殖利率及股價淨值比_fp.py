##----- pe is '0.00' when pe < 0 -----

import pandas as pd
import datetime, json, sys
sys.path.append('C:/Users/user/Dropbox/program/mypackage')
import crawler_fp as crawler

def getPlainText(lastdate, t, type):
    input_date = crawler.inputDate(lastdate, t)
    url = 'http://www.twse.com.tw/exchangeReport/BWIBBU_d?response=json&date={}&selectType={}'.format(input_date, type)
    return crawler.plainText_get(url)

def priceEarning(lastdate, t, type):
    d = json.loads(getPlainText(lastdate, t, type))
    data = d['data']
    fields = d['fields']
    date = d['date'][0:4] + '-' + d['date'][4:6] + '-' + d['date'][6:]
    df = pd.DataFrame(data, columns=fields).replace(',', '', regex=True).replace('--', 'NaN').replace('-', 'NaN')
    df['證券代號'] = df['證券代號'].str.strip()
    df['證券名稱'] = df['證券名稱'].str.strip()
    df.insert(0, '年月日', date)
    df['年月日'] = pd.to_datetime(df['年月日']).astype(str)
    floatColumns = ['殖利率(%)', '本益比', '股價淨值比']
    df[floatColumns] = df[floatColumns].astype(float)
    columns = ['年月日', '證券代號', '證券名稱', '殖利率(%)', '股利年度', '本益比', '股價淨值比', '財報年/季']
    if '股利年度' and '財報年/季' in list(df):
        intColumns = ['股利年度']
        df[intColumns] = df[intColumns].astype(int)
        df[floatColumns] = df[floatColumns].astype(float)
        df.股利年度 = df.股利年度 + 1911
        df['財報年/季'] = (df['財報年/季'].str.split('/').str[0].astype(int) + 1911).astype(str) + '/' + df['財報年/季'].str.split('/').str[1]
        df = df[columns]
    elif '財報年/季' in list(df):
        df['股利年度'] = 'NaN'
        df['財報年/季'] = (df['財報年/季'].str.split('/').str[0].astype(int) + 1911).astype(str) + '/' + df['財報年/季'].str.split('/').str[1]
        df = df[columns]
    else:
        df['股利年度'] = 'NaN'
        df['財報年/季'] = 'NaN'
        df = df[columns]
    return df

g = crawler.crawToSqlite(priceEarning, '個股日本益比、殖利率及股價淨值比', 'ALL')
# next(g)
for _ in g: pass