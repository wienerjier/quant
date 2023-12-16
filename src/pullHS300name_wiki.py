"""
从wiki中爬取沪深300的股票列表, 存入Mysql中hs300name_wiki表
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup
import datetime
import mysql.connector

# 连接到MySQL数据库
conn = mysql.connector.connect(
    host='localhost',
    user='wieneralan',
    password='0922',
    database='stock_data'
)
# 创建游标
cursor = conn.cursor()


#now = datetime.datetime.utcnow()
now = datetime.date.today()
#url = 'http://quote.eastmoney.com/center/gridlist.html'
# 从wiki爬 沪深300 的名字列表
response = requests.get("https://zh.wikipedia.org/wiki/%E6%B2%AA%E6%B7%B1300")
soup = BeautifulSoup(response.text, 'html.parser')

# 取出股票的symbol与名字
symbolslist = soup.select('table')[1].select('tr')[1:]
print(symbolslist)
symbols=[]
for i, symbol in enumerate(symbolslist):
    tds = symbol.select('td')
    exchange_id = 'sh' if("上海" in tds[2].text) else 'sz'
    code = tds[0].text
    name = tds[1].select('a') [0].text
    exchange_name = tds[2].text
    weight = tds[3].text

    # symbols.append((code, exchange_id, name, exchange_name, weight, now))
    print(tds[0].text,tds[1].select('a')[0].text,tds[2].text,exchange_id,tds[3].text)

    # 存入sql
    sql = f"INSERT INTO hs300name_wiki_test (code, exchange_id, name, exchange_name, weight, update_date) VALUES " \
          f"('{code}', '{exchange_id}', '{name}', '{exchange_name}', '{weight}', '{now}')"
    cursor.execute(sql)


# sql提交更改 sql关闭游标和连接
conn.commit()
cursor.close()
conn.close()

#存到csv
# result = pd.DataFrame(symbols)
# result.to_csv("/home/wieneralan/software/quant/hs300_stocks.csv", encoding="gbk", index=False)
#print(symbols)



