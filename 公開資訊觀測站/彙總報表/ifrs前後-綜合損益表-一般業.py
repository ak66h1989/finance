from sqlite3 import *
import os
from numpy import *
from pandas import *
from functools import *

def mymerge(x, y):
    m = merge(x, y, on=[col for col in list(x) if col in list(y)], how='outer')
    return m

import os
import re

path = 'C:/Users/ak66h_000/Documents/db/'
os.chdir(path)

conn = connect('summary.sqlite3')
c = conn.cursor()
ind=['一般業', '銀行業', '金控業', '證券業', '保險業', '其他業', '未知業']

col = {'營業毛利(毛損)': '營業毛利（毛損）淨額', '營業淨利(淨損)': '營業利益（損失）', '營業外費用及損失': '營業外收入及支出','繼續營業單位稅前淨利(淨損)': '稅前淨利（淨損）', '所得稅費用(利益)': '所得稅費用（利益）', '繼續營業單位淨利(淨損)': '繼續營業單位本期淨利（淨損）', '非常損益':'其他綜合損益（淨額）','合併總損益':'本期綜合損益總額','合併淨損益':'綜合損益總額歸屬於母公司業主','共同控制下前手權益損益':'綜合損益總額歸屬於共同控制下前手權益','少數股權損益':'綜合損益總額歸屬於非控制權益','基本每股盈餘':'基本每股盈餘（元）'}
df10 = read_sql_query("SELECT * from `綜合損益表-一般業`", conn)
df11 = read_sql_query("SELECT * from `ifrs前-合併損益表-一般業`", conn)
df11 = df11.rename(columns= col)
df = mymerge(df10, df11)
list(df10)
list(df11)
list(df)
[x for x in list(df10) if x not in list(df11)]
[x for x in list(df11) if x not in list(df10)]

col1=['年',
 '季',
 '公司代號',
 '公司名稱',
 '營業收入',
 '營業成本',
 '營業毛利（毛損）',
 '未實現銷貨（損）益',
 '已實現銷貨（損）益',
 '營業毛利（毛損）淨額',
 '營業費用',
 '其他收益及費損淨額',
 '營業利益（損失）',
 '營業外收入及支出',
 '營業外收入及利益',
 '稅前淨利（淨損）',
 '所得稅費用（利益）',
 '繼續營業單位本期淨利（淨損）',
 '停業單位損益',
 '合併前非屬共同控制股權損益',
 '本期淨利（淨損）',
 '其他綜合損益（淨額）',
 '合併前非屬共同控制股權綜合損益淨額',
 '會計原則變動累積影響數',
 '本期綜合損益總額',
 '淨利（淨損）歸屬於母公司業主',
 '淨利（淨損）歸屬於共同控制下前手權益',
 '淨利（淨損）歸屬於非控制權益',
 '綜合損益總額歸屬於母公司業主',
 '綜合損益總額歸屬於共同控制下前手權益',
 '綜合損益總額歸屬於非控制權益',
 '基本每股盈餘（元）',
 ]
df=df[col1]

df.dtypes
table = 'ifrs前後-綜合損益表-' + '一般業'
df = df.sort_values(['年', '季', '公司代號']).reset_index(drop=True)
sql = 'ALTER TABLE `%s` RENAME TO `%s0`' % (table, table)
c.execute(sql)
sql = 'create table `%s` (`%s`, PRIMARY KEY (%s))' % (table, '`,`'.join(list(df)), '`年`, `季`, `公司代號`')
c.execute(sql)
sql = 'insert into `%s`(`%s`) values(%s)' % (table, '`,`'.join(list(df)), ','.join('?' * len(list(df))))
c.executemany(sql, df.values.tolist())
conn.commit()
sql = "drop table `%s0`" % table
c.execute(sql)

# ,'':''
# df11 = read_sql_query("SELECT * from `ifrs前後-綜合損益表-一般業`", conn)
#
# col2={
#  '營業成本':'  營業成本',
#  '未實現銷貨（損）益':'  未實現銷貨（損）益',
#  '已實現銷貨（損）益':'  已實現銷貨（損）益',
#  '營業費用':'  營業費用',
#  '其他收益及費損淨額':'  其他收益及費損淨額',
#  '營業外收入及支出':'  營業外收入及支出',
#  '營業外收入及利益':'  營業外收入及利益',
#  '所得稅費用（利益）':'  所得稅費用（利益）',
#  '停業單位損益':'  停業單位損益',
#  '合併前非屬共同控制股權損益':'  合併前非屬共同控制股權損益',
#  '其他綜合損益（淨額）':'  其他綜合損益（淨額）',
#  '合併前非屬共同控制股權綜合損益淨額':'  合併前非屬共同控制股權綜合損益淨額',
#  '會計原則變動累積影響數':'  會計原則變動累積影響數'
#       }
#
# col2 = {
#     '營業成本': '&emsp;&emsp;營業成本',
#     '未實現銷貨（損）益': '&emsp;&emsp;未實現銷貨（損）益',
#     '已實現銷貨（損）益': '&emsp;&emsp;已實現銷貨（損）益',
#     '營業費用': '&emsp;&emsp;營業費用',
#     '其他收益及費損淨額': '&emsp;&emsp;其他收益及費損淨額',
#     '營業外收入及支出': '&emsp;&emsp;營業外收入及支出',
#     '營業外收入及利益': '&emsp;&emsp;營業外收入及利益',
#     '所得稅費用（利益）': '&emsp;&emsp;所得稅費用（利益）',
#     '停業單位損益': '&emsp;&emsp;停業單位損益',
#     '合併前非屬共同控制股權損益': '&emsp;&emsp;合併前非屬共同控制股權損益',
#     '其他綜合損益（淨額）': '&emsp;&emsp;其他綜合損益（淨額）',
#     '合併前非屬共同控制股權綜合損益淨額': '&emsp;&emsp;合併前非屬共同控制股權綜合損益淨額',
#     '會計原則變動累積影響數': '&emsp;&emsp;會計原則變動累積影響數'
# }
# df11 = df11.rename(columns= col2)