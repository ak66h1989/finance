import pandas as pd
import datetime, json, sys
sys.path.append('C:/Users/user/Dropbox/program/mypackage_py')
import crawler_fp as crawler

def getPlainText(lastdate, t, type):
    input_date = crawler.inputDate(lastdate, t)
    url = 'http://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={}&type={}'.format(input_date, type)
    return crawler.plainText_get(url)

###----每日收盤行情(全部(不含權證、牛熊證))----

def close(lastdate, t, type):
    d = json.loads(getPlainText(lastdate, t, type))
    data = d['data5']
    fields = d['fields5']
    date = d['date'][0:4] + '-' + d['date'][4:6] + '-' + d['date'][6:]
    df = pd.DataFrame(data, columns=fields).replace(',', '', regex=True).replace('--', 'NaN')
    df['漲跌(+/-)'] = df['漲跌(+/-)'].replace('<p style= color:red>+</p>', 1).replace('<p style= color:green>-</p>',-1).replace('X', 'NaN').replace(' ',0)
    df.insert(0, '年月日', date)
    df['年月日'] = pd.to_datetime(df['年月日']).astype(str)
    floatColumns = ['成交股數', '成交筆數', '成交金額', '開盤價', '最高價', '最低價', '收盤價', '漲跌(+/-)', '漲跌價差', '最後揭示買價', '最後揭示買量', '最後揭示賣價', '最後揭示賣量', '本益比']
    df[floatColumns] = df[floatColumns].astype(float)
    return df

g = crawler.crawToSqlite(close, '每日收盤行情(全部(不含權證、牛熊證))', 'ALLBUT0999')
# next(g)
for _ in g: pass

###----大盤統計資訊----

def market(lastdate, t, type):
    d = json.loads(getPlainText(lastdate, t, type))
    data = d['data1']
    fields = d['fields1']
    date = d['date'][0:4] + '-' + d['date'][4:6] + '-' + d['date'][6:]
    df = pd.DataFrame(data, columns=fields).replace(',', '', regex=True).replace('--', 'NaN')
    df['漲跌(+/-)'] = df['漲跌(+/-)'].replace("<p style ='color:red'>+</p>", 1).replace("<p style ='color:green'>-</p>", -1).replace('X', 'NaN').replace(' ', 0)
    df.insert(0, '年月日', date)
    df['年月日'] = pd.to_datetime(df['年月日']).astype(str)
    floatColumns = ['收盤指數', '漲跌(+/-)', '漲跌點數', '漲跌百分比(%)']
    df[floatColumns] = df[floatColumns].astype(float)
    return df

g = crawler.crawToSqlite(market, '大盤統計資訊', 'ALLBUT0999')
# next(g)
for _ in g: pass

###----大盤統計資訊(報酬指數)----

def marketReturn(lastdate, t, type):
    d = json.loads(getPlainText(lastdate, t, type))
    data = d['data2']
    fields = d['fields2']
    date = d['date'][0:4] + '-' + d['date'][4:6] + '-' + d['date'][6:]
    df = pd.DataFrame(data, columns=fields).replace(',', '', regex=True).replace('--', 'NaN')
    df['漲跌(+/-)'] = df['漲跌(+/-)'].replace("<p style ='color:red'>+</p>", 1).replace("<p style ='color:green'>-</p>",-1).replace('X', 'NaN').replace(' ', 0)
    df.insert(0, '年月日', date)
    df = df.rename(columns={'報酬指數': '指數'})
    df['年月日'] = pd.to_datetime(df['年月日']).astype(str)
    floatColumns = ['收盤指數', '漲跌(+/-)', '漲跌點數', '漲跌百分比(%)']
    df[floatColumns] = df[floatColumns].astype(float)
    return df

g = crawler.crawToSqlite(marketReturn, '大盤統計資訊', 'ALLBUT0999')
# next(g)
for _ in g: pass

###----大盤成交統計----

def marketDeal(lastdate, t, type):
    d = json.loads(getPlainText(lastdate, t, type))
    data = d['data3']
    fields = d['fields3']
    date = d['date'][0:4] + '-' + d['date'][4:6] + '-' + d['date'][6:]
    df = pd.DataFrame(data, columns=fields).replace(',', '', regex=True).replace('--', 'NaN')
    df.insert(0, '年月日', date)
    df['年月日'] = pd.to_datetime(df['年月日']).astype(str)
    floatColumns = ['成交金額(元)', '成交股數(股)', '成交筆數']
    df[floatColumns] = df[floatColumns].astype(float)
    return df

g = crawler.crawToSqlite(marketDeal, '大盤成交統計', 'ALLBUT0999')
# next(g)
for _ in g: pass

###----漲跌證券數合計----

def upsAndDown(lastdate, t, type):
    d = json.loads(getPlainText(lastdate, t, type))
    data = d['data4']
    fields = d['fields4']
    date = d['date'][0:4] + '-' + d['date'][4:6] + '-' + d['date'][6:]
    data[0][1].split('(')[0]
    L = []
    l = data[0]
    L.append([i.split('(')[0] for i in l])
    L.append([i.split('(')[1].replace(')', '') for i in l])
    l = data[1]
    L.append([i.split('(')[0] for i in l])
    L.append([i.split('(')[1].replace(')', '') for i in l])
    L.append(data[2])
    L.append(data[3])
    L.append(data[4])
    df = pd.DataFrame(L, columns=fields).replace(',', '', regex=True).replace('--', 'NaN')
    df.insert(0, '年月日', date)
    df['年月日'] = pd.to_datetime(df['年月日']).astype(str)
    intColumns = ['整體市場', '股票']
    df[intColumns] = df[intColumns].astype(int)
    return df

g = crawler.crawToSqlite(upsAndDown, '漲跌證券數合計', 'ALLBUT0999')
# next(g)
for _ in g: pass

###----牛證(不含可展延牛證)----

def callableBull(lastdate, t, type):
    d = json.loads(getPlainText(lastdate, t, type))
    data = d['data1']
    fields = d['fields1']
    date = d['date'][0:4] + '-' + d['date'][4:6] + '-' + d['date'][6:]
    df = pd.DataFrame(data, columns=fields).replace(',', '', regex=True).replace('--', 'NaN')
    df.insert(0, '年月日', date)
    df['年月日'] = pd.to_datetime(df['年月日']).astype(str)
    df['漲跌(+/-)'] = df['漲跌(+/-)'].replace("<p style= color:red>+</p>", 1).replace("<p style= color:green>-</p>",-1).replace('X', 'NaN').replace(' ', 0)
    df['牛熊證觸及限制價格'] = df['牛熊證觸及限制價格'].replace('', 'NaN').fillna('NaN')
    df['本益比'] = df['本益比'].replace('', 'NaN').fillna('NaN')
    intColumns = ['成交股數', '成交筆數', '最後揭示買量', '最後揭示賣量']
    floatColumns = ['成交金額', '開盤價', '最高價', '最低價', '收盤價', '漲跌(+/-)', '漲跌價差', '最後揭示買價', '最後揭示賣價', '本益比', '牛熊證觸及限制價格', '標的證券收盤價/指數']
    df[intColumns] = df[intColumns].astype(int)
    df[floatColumns] = df[floatColumns].astype(float)
    return df

g = crawler.crawToSqlite(callableBull, '牛證(不含可展延牛證)', '0999C')
# next(g)
for _ in g: pass

###----熊證(不含可展延熊證)----

def callableBear(lastdate, t, type):
    d = json.loads(getPlainText(lastdate, t, type))
    data = d['data1']
    fields = d['fields1']
    date = d['date'][0:4] + '-' + d['date'][4:6] + '-' + d['date'][6:]
    df = pd.DataFrame(data, columns=fields).replace(',', '', regex=True).replace('--', 'NaN')
    df.insert(0, '年月日', date)
    df['年月日'] = pd.to_datetime(df['年月日']).astype(str)
    df['漲跌(+/-)'] = df['漲跌(+/-)'].replace("<p style= color:red>+</p>", 1).replace("<p style= color:green>-</p>", -1).replace('X', 'NaN').replace(' ', 0)
    df['牛熊證觸及限制價格'] = df['牛熊證觸及限制價格'].replace('', 'NaN').fillna('NaN')
    df['本益比'] = df['本益比'].replace('', 'NaN').fillna('NaN')
    intColumns = ['成交股數', '成交筆數', '最後揭示買量', '最後揭示賣量']
    floatColumns = ['成交金額', '開盤價', '最高價', '最低價', '收盤價', '漲跌(+/-)', '漲跌價差', '最後揭示買價', '最後揭示賣價', '本益比', '牛熊證觸及限制價格','標的證券收盤價/指數']
    df[intColumns] = df[intColumns].astype(int)
    df[floatColumns] = df[floatColumns].astype(float)
    return df

g = crawler.crawToSqlite(callableBear, '熊證(不含可展延熊證)', '0999B')
# next(g)
for _ in g: pass

###----可展延牛證----

def extendedCallableBear(lastdate, t, type):
    d = json.loads(getPlainText(lastdate, t, type))
    data = d['data1']
    fields = d['fields1']
    date = d['date'][0:4] + '-' + d['date'][4:6] + '-' + d['date'][6:]
    df = pd.DataFrame(data, columns=fields).replace(',', '', regex=True).replace('--', 'NaN')
    df.insert(0, '年月日', date)
    df['年月日'] = pd.to_datetime(df['年月日']).astype(str)
    df['漲跌(+/-)'] = df['漲跌(+/-)'].replace("<p style= color:red>+</p>", 1).replace("<p style= color:green>-</p>", -1).replace('X', 'NaN').replace(' ', 0)
    df['牛熊證觸及限制價格'] = df['牛熊證觸及限制價格'].replace('', 'NaN').fillna('NaN')
    df['本益比'] = df['本益比'].replace('', 'NaN').fillna('NaN')
    intColumns = ['成交股數', '成交筆數', '最後揭示買量', '最後揭示賣量']
    floatColumns = ['成交金額', '開盤價', '最高價', '最低價', '收盤價', '漲跌(+/-)', '漲跌價差', '最後揭示買價', '最後揭示賣價', '本益比', '牛熊證觸及限制價格',
                    '標的證券收盤價/指數']
    df[intColumns] = df[intColumns].astype(int)
    df[floatColumns] = df[floatColumns].astype(float)
    return df

g = crawler.crawToSqlite(extendedCallableBear, '可展延牛證', '0999X')
# next(g)
for _ in g: pass
