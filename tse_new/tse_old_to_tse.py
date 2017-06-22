##----- pe is '0.00' when pe < 0 -----

from sqlite3 import *
connLite = connect('C:\\Users\\ak66h_000\\Documents\\db\\tse.sqlite3')
c = connLite.cursor()

import requests
from bs4 import BeautifulSoup
from numpy import *
from pandas import *
import re
import datetime
import json
import sys, os
sys.path.append('C:/Users/ak66h_000/Dropbox/program/mypackage')
import psycopg2
import sqlCommand as sqlCommand

get_option("display.max_rows")
get_option("display.max_columns")
set_option("display.max_rows", 100)
set_option("display.max_columns", 1000)
set_option('display.expand_frame_repr', False)
set_option('display.unicode.east_asian_width', True)

tablename = '每日收盤行情(全部(不含權證、牛熊證))'
sql = "SELECT * FROM '{}'"
df = read_sql_query(sql.format('每日收盤行情(全部(不含權證、牛熊證))'), connLite).replace('--', 'NaN')
df['漲跌(+/-)'] = df['漲跌(+/-)'].replace('－', -1).replace('＋', 1).replace('X', 'NaN').replace(' ', '0').fillna('NaN')
df['年月日'] = to_datetime(df['年月日']).astype(str)
floatColumns = ['成交股數', '成交筆數', '成交金額', '開盤價', '最高價', '最低價', '收盤價', '漲跌(+/-)', '漲跌價差', '最後揭示買價', '最後揭示買量', '最後揭示賣價', '最後揭示賣量', '本益比']
intColumns = []
df[intColumns] = df[intColumns].astype(int)
df[floatColumns] = df[floatColumns].astype(float)
df.dtypes

tablename_new = tablename + '0'
sqlCommand.renameTable(tablename, tablename_new, connLite)
columns = list(df)
primaryKeys = ['年月日', '證券代號']
sqlCommand.createTable(tablename, columns, primaryKeys, connLite)
sqlCommand.insertData(tablename, df, connLite)
sqlCommand.dropTable(tablename_new, connLite)


tablename = '個股日本益比、殖利率及股價淨值比'
sql = "SELECT * FROM '{}'"
df = read_sql_query(sql.format(tablename), connLite).replace('--', 'NaN').fillna('NaN')
print(df)
df['本益比'] = df['本益比'].replace('-', 'NaN')
df['股價淨值比'] = df['股價淨值比'].replace('-', 'NaN')
df['年月日'] = to_datetime(df['年月日']).astype(str)
floatColumns = ['本益比', '殖利率(%)', '股價淨值比']
# for col in floatColumns:
#     print(col)
#     df[col].astype(float)

intColumns = []
df[intColumns] = df[intColumns].astype(int)
df[floatColumns] = df[floatColumns].astype(float)
df.dtypes

tablename_new = tablename + '0'
sqlCommand.renameTable(tablename, tablename_new, connLite)
columns = list(df)
primaryKeys = ['年月日', '證券代號']
sqlCommand.createTable(tablename, columns, primaryKeys, connLite)
sqlCommand.insertData(tablename, df, connLite)
sqlCommand.dropTable(tablename_new, connLite)


tablename = '三大法人買賣超日報(股)'
sql = "SELECT * FROM '{}'"
df = read_sql_query(sql.format(tablename), connLite).replace('--', 'NaN').fillna('NaN')
print(df)
df['年月日'] = to_datetime(df['年月日']).astype(str)
floatColumns = ['外資買進股數','外資賣出股數','投信買進股數','投信賣出股數','自營商買進股數(自行買賣)','自營商賣出股數(自行買賣)','自營商買進股數(避險)','自營商賣出股數(避險)','自營商買進股數','自營商賣出股數','三大法人買賣超股數']

intColumns = []
df[intColumns] = df[intColumns].astype(int)
df[floatColumns] = df[floatColumns].astype(float)
df.dtypes

tablename_new = tablename + '0'
sqlCommand.renameTable(tablename, tablename_new, connLite)
columns = list(df)
primaryKeys = ['年月日', '證券代號']
sqlCommand.createTable(tablename, columns, primaryKeys, connLite)
sqlCommand.insertData(tablename, df, connLite)
sqlCommand.dropTable(tablename_new, connLite)


tablename = '外資及陸資買賣超彙總表 (股)'
sql = "SELECT * FROM '{}'"
df = read_sql_query(sql.format(tablename), connLite).replace('--', 'NaN').fillna('NaN')
print(df)
df['鉅額交易'] = df['鉅額交易'].replace(' ', 0).replace('*', 1)
df['年月日'] = to_datetime(df['年月日']).astype(str)
intColumns = ['鉅額交易', '買進股數', '賣出股數', '買賣超股數']
floatColumns = []
df[intColumns] = df[intColumns].astype(int)
df[floatColumns] = df[floatColumns].astype(float)
df.dtypes
set(df.鉅額交易)
tablename_new = tablename + '0'
sqlCommand.renameTable(tablename, tablename_new, connLite)
columns = list(df)
primaryKeys = ['年月日', '證券代號']
sqlCommand.createTable(tablename, columns, primaryKeys, connLite)
sqlCommand.insertData(tablename, df, connLite)
sqlCommand.dropTable(tablename_new, connLite)


tablename = '投信買賣超彙總表 (股)'
sql = "SELECT * FROM '{}'"
df = read_sql_query(sql.format(tablename), connLite).replace('--', 'NaN').fillna('NaN')
print(df)
df['鉅額交易'] = df['鉅額交易'].replace(' ', 0).replace('*', 1)
df['年月日'] = to_datetime(df['年月日']).astype(str)
intColumns = ['鉅額交易', '買進股數', '賣出股數', '買賣超股數']
floatColumns = []
df[intColumns] = df[intColumns].astype(int)
df[floatColumns] = df[floatColumns].astype(float)
df.dtypes
set(df.鉅額交易)
tablename_new = tablename + '0'
sqlCommand.renameTable(tablename, tablename_new, connLite)
columns = list(df)
primaryKeys = ['年月日', '證券代號']
sqlCommand.createTable(tablename, columns, primaryKeys, connLite)
sqlCommand.insertData(tablename, df, connLite)
sqlCommand.dropTable(tablename_new, connLite)


tablename = '當日融券賣出與借券賣出成交量值(元)'
sql = "SELECT * FROM '{}'"
df = read_sql_query(sql.format(tablename), connLite).replace('--', 'NaN').fillna('NaN')
print(df)
# df['鉅額交易'] = df['鉅額交易'].replace(' ', 0).replace('*', 1)
df['年月日'] = to_datetime(df['年月日']).astype(str)
intColumns = ['融券賣出成交數量', '借券賣出成交數量']
floatColumns = ['融券賣出成交金額', '借券賣出成交金額']
df[intColumns] = df[intColumns].astype(int)
df[floatColumns] = df[floatColumns].astype(float)
df.dtypes
set(df.鉅額交易)
tablename_new = tablename + '0'
sqlCommand.renameTable(tablename, tablename_new, connLite)
columns = list(df)
primaryKeys = ['年月日', '證券代號']
sqlCommand.createTable(tablename, columns, primaryKeys, connLite)
sqlCommand.insertData(tablename, df, connLite)
sqlCommand.dropTable(tablename_new, connLite)


tablename = '可展延牛證'
sql = "SELECT * FROM '{}'"
df = read_sql_query(sql.format(tablename), connLite).replace('--', 'NaN').fillna('NaN')
print(df)
df['漲跌(+/-)'] = df['漲跌(+/-)'].replace('－', -1).replace('＋', 1).replace('X', 'NaN').replace(' ', '0').fillna('NaN')
df['牛熊證觸及限制價格'] = df['牛熊證觸及限制價格'].replace('\xa0', 'NaN').fillna('NaN')
df['本益比'] = df['本益比'].replace('\xa0', 'NaN').fillna('NaN')
df['年月日'] = to_datetime(df['年月日']).astype(str)
intColumns = ['成交股數','成交筆數','最後揭示買量','最後揭示賣量']
floatColumns = ['成交金額','開盤價','最高價','最低價','收盤價','漲跌(+/-)','漲跌價差','最後揭示買價','最後揭示賣價','本益比','牛熊證觸及限制價格','標的證券收盤價/指數']
df[intColumns] = df[intColumns].astype(int)
df[floatColumns] = df[floatColumns].astype(float)

df.dtypes
set(df.牛熊證觸及限制價格)
set(df.本益比)

tablename_new = tablename + '0'
sqlCommand.renameTable(tablename, tablename_new, connLite)
columns = list(df)
primaryKeys = ['年月日', '證券代號']
sqlCommand.createTable(tablename, columns, primaryKeys, connLite)
sqlCommand.insertData(tablename, df, connLite)
sqlCommand.dropTable(tablename_new, connLite)


tablename = '熊證(不含可展延熊證)'
sql = "SELECT * FROM '{}'"
df = read_sql_query(sql.format(tablename), connLite).replace('--', 'NaN').fillna('NaN')
print(df)
df['漲跌(+/-)'] = df['漲跌(+/-)'].replace('－', -1).replace('＋', 1).replace('X', 'NaN').replace(' ', '0').fillna('NaN')
df['牛熊證觸及限制價格'] = df['牛熊證觸及限制價格'].replace('\xa0', 'NaN').fillna('NaN')
df['本益比'] = df['本益比'].replace('\xa0', 'NaN').fillna('NaN')
df['年月日'] = to_datetime(df['年月日']).astype(str)
intColumns = ['成交股數','成交筆數','最後揭示買量','最後揭示賣量']
floatColumns = ['成交金額','開盤價','最高價','最低價','收盤價','漲跌(+/-)','漲跌價差','最後揭示買價','最後揭示賣價','本益比','牛熊證觸及限制價格','標的證券收盤價/指數']
df[intColumns] = df[intColumns].astype(int)
df[floatColumns] = df[floatColumns].astype(float)
df.dtypes
tablename_new = tablename + '0'
sqlCommand.renameTable(tablename, tablename_new, connLite)
columns = list(df)
primaryKeys = ['年月日', '證券代號']
sqlCommand.createTable(tablename, columns, primaryKeys, connLite)
sqlCommand.insertData(tablename, df, connLite)
sqlCommand.dropTable(tablename_new, connLite)


tablename = '牛證(不含可展延牛證)'
sql = "SELECT * FROM '{}'"
df = read_sql_query(sql.format(tablename), connLite).replace('--', 'NaN').fillna('NaN')
print(df)
df['漲跌(+/-)'] = df['漲跌(+/-)'].replace('－', -1).replace('＋', 1).replace('X', 'NaN').replace(' ', '0').fillna('NaN')
df['牛熊證觸及限制價格'] = df['牛熊證觸及限制價格'].replace('\xa0', 'NaN').fillna('NaN')
df['本益比'] = df['本益比'].replace('\xa0', 'NaN').fillna('NaN')
df['年月日'] = to_datetime(df['年月日']).astype(str)
intColumns = ['成交股數','成交筆數','最後揭示買量','最後揭示賣量']
floatColumns = ['成交金額','開盤價','最高價','最低價','收盤價','漲跌(+/-)','漲跌價差','最後揭示買價','最後揭示賣價','本益比','牛熊證觸及限制價格','標的證券收盤價/指數']
df[intColumns] = df[intColumns].astype(int)
df[floatColumns] = df[floatColumns].astype(float)
df.dtypes
tablename_new = tablename + '0'
sqlCommand.renameTable(tablename, tablename_new, connLite)
columns = list(df)
primaryKeys = ['年月日', '證券代號']
sqlCommand.createTable(tablename, columns, primaryKeys, connLite)
sqlCommand.insertData(tablename, df, connLite)
sqlCommand.dropTable(tablename_new, connLite)


tablename = '大盤成交統計'
sql = "SELECT * FROM '{}'"
df = read_sql_query(sql.format(tablename), connLite).replace('--', 'NaN').fillna('NaN')
print(df)
df['年月日'] = to_datetime(df['年月日']).astype(str)
intColumns = []
floatColumns = ['成交金額(元)','成交股數(股)','成交筆數']
df[intColumns] = df[intColumns].astype(int)
df[floatColumns] = df[floatColumns].astype(float)
df.dtypes
tablename_new = tablename + '0'
sqlCommand.renameTable(tablename, tablename_new, connLite)
columns = list(df)
primaryKeys = ['年月日', '成交統計']
sqlCommand.createTable(tablename, columns, primaryKeys, connLite)
sqlCommand.insertData(tablename, df, connLite)
sqlCommand.dropTable(tablename_new, connLite)


tablename = '大盤統計資訊'
sql = "SELECT * FROM '{}'"
df = read_sql_query(sql.format(tablename), connLite).replace('---', 'NaN').replace('--', 'NaN').fillna('NaN')
print(df)
df['年月日'] = to_datetime(df['年月日']).astype(str)
df['漲跌(+/-)'] = df['漲跌(+/-)'].replace('－', -1).replace('＋', 1).replace('X', 'NaN').replace(' ', '0').fillna('NaN')
intColumns = []
floatColumns = ['收盤指數','漲跌(+/-)','漲跌點數','漲跌百分比(%)']
df[intColumns] = df[intColumns].astype(int)
df[floatColumns] = df[floatColumns].astype(float)
df.dtypes
set(df['漲跌(+/-)'])
tablename_new = tablename + '0'
sqlCommand.renameTable(tablename, tablename_new, connLite)
columns = list(df)
primaryKeys = ['年月日', '指數']
sqlCommand.createTable(tablename, columns, primaryKeys, connLite)
sqlCommand.insertData(tablename, df, connLite)
sqlCommand.dropTable(tablename_new, connLite)


tablename = '除權息計算結果表'
sql = "SELECT * FROM '{}'"
df = read_sql_query(sql.format(tablename), connLite).replace('---', 'NaN').replace('--', 'NaN').fillna('NaN')
print(df)
df['年月日'] = to_datetime(df['年月日']).astype(str)
intColumns = []
floatColumns = ['除權息前收盤價','除權息參考價','權值+息值']
df[intColumns] = df[intColumns].astype(int)
df[floatColumns] = df[floatColumns].astype(float)
df.dtypes
tablename_new = tablename + '0'
sqlCommand.renameTable(tablename, tablename_new, connLite)
columns = list(df)
primaryKeys = ['年月日', '股票代號']
sqlCommand.createTable(tablename, columns, primaryKeys, connLite)
sqlCommand.insertData(tablename, df, connLite)
sqlCommand.dropTable(tablename_new, connLite)


tablename = 'index'
sql = "SELECT * FROM '{}'"
df = read_sql_query(sql.format(tablename), connLite).replace('---', 'NaN').replace('--', 'NaN').fillna('NaN')
print(df)
df['年月日'] = to_datetime(df['年月日']).astype(str)
intColumns = []
floatColumns = ['中小型A級動能50報酬指數','中小型A級動能50指數','中小型精選50報酬指數','中小型精選50指數','低波動精選 30 報酬指數','低波動精選30指數','低貝塔 100 報酬指數','低貝塔100指數','光電類報酬指數','光電類指數','其他電子類報酬指數','其他電子類指數','其他類報酬指數','其他類指數','化學生技醫療類報酬指數','化學生技醫療類指數','化學類報酬指數','化學類指數','半導體類報酬指數','半導體類指數','塑膠化工類指數','塑膠類報酬指數','塑膠類指數','寶島股價報酬指數','寶島股價指數','小型股300報酬指數','小型股300指數','工業菁英 30 報酬指數','工業菁英30指數','建材營造類報酬指數','建材營造類指數','未含金融保險股指數','未含金融電子股報酬指數','未含金融電子股指數','未含電子股指數','機電類指數','橡膠類報酬指數','橡膠類指數','水泥窯製類指數','水泥類報酬指數','水泥類指數','汽車類報酬指數','汽車類指數','油電燃氣類報酬指數','油電燃氣類指數','漲升股利100報酬指數','漲升股利100指數','漲升股利150報酬指數','漲升股利150指數','玻璃陶瓷類報酬指數','玻璃陶瓷類指數','生技醫療類報酬指數','生技醫療類指數','發行量加權股價報酬指數','發行量加權股價指數','紡織纖維類報酬指數','紡織纖維類指數','臺指日報酬兩倍指數','臺指日報酬反向一倍指數','臺灣50報酬指數','臺灣50指數','臺灣中型100報酬指數','臺灣中型100指數','臺灣企業經營101報酬指數','臺灣企業經營101指數','臺灣低波動高股息報酬指數','臺灣低波動高股息指數','臺灣公司治理100報酬指數','臺灣公司治理100指數','臺灣就業99報酬指數','臺灣就業99指數','臺灣發達報酬指數','臺灣發達指數','臺灣資訊科技報酬指數','臺灣資訊科技指數','臺灣高股息報酬指數','臺灣高股息指數','臺灣高薪100報酬指數','臺灣高薪100指數','航運類報酬指數','航運類指數','藍籌 30 報酬指數','藍籌30反向一倍指數','藍籌30指數','觀光類報酬指數','觀光類指數','貿易百貨類報酬指數','貿易百貨類指數','資訊服務類報酬指數','資訊服務類指數','通信網路類報酬指數','通信網路類指數','造紙類報酬指數','造紙類指數','金融保險類報酬指數','金融保險類指數','鋼鐵類報酬指數','鋼鐵類指數','電器電纜類報酬指數','電器電纜類指數','電子菁英 30 報酬指數','電子菁英30指數','電子通路類報酬指數','電子通路類指數','電子零組件類報酬指數','電子零組件類指數','電子類兩倍槓桿指數','電子類反向指數','電子類報酬指數','電子類指數','電機機械類報酬指數','電機機械類指數','電腦及週邊設備類報酬指數','電腦及週邊設備類指數','食品類報酬指數','食品類指數']
df[intColumns] = df[intColumns].astype(int)
df[floatColumns] = df[floatColumns].astype(float)
df.dtypes
tablename_new = tablename + '0'
sqlCommand.renameTable(tablename, tablename_new, connLite)
columns = list(df)
primaryKeys = ['年月日']
sqlCommand.createTable(tablename, columns, primaryKeys, connLite)
sqlCommand.insertData(tablename, df, connLite)
sqlCommand.dropTable(tablename_new, connLite)


# tablename = '自營商買賣超彙總表 (股)'
# sql = "SELECT * FROM '{}'"
# df = read_sql_query(sql.format(tablename), connLite)
# df = df.fillna('NaN')
# print(df)
# df['年月日'] = to_datetime(df['年月日']).astype(str)
# intColumns = ['自營商(自行買賣)賣出股數','自營商(自行買賣)買賣超股數','自營商(自行買賣)買進股數','自營商(避險)賣出股數','自營商(避險)買賣超股數','自營商(避險)買進股數','自營商賣出股數','自營商買賣超股數','自營商買進股數']
# floatColumns = []
# df[intColumns] = df[intColumns].astype(int)
# df[floatColumns] = df[floatColumns].astype(float)
# df.dtypes
# tablename_new = tablename + '0'
# sqlCommand.renameTable(tablename, tablename_new, connLite)
# columns = list(df)
# primaryKeys = ['年月日', '證券代號']
# sqlCommand.createTable(tablename, columns, primaryKeys, connLite)
# sqlCommand.insertData(tablename, df, connLite)
# sqlCommand.dropTable(tablename_new, connLite)