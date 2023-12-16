"""
使用baostock爬取沪深300的股票列表,实际只爬了150个, 存入Mysql中hs150name_BaoStock表
"""
import pandas as pd
import numpy as np
import baostock as bs
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


# 登陆系统
lg = bs.login()
# 显示登陆返回信息
print('login respond error_code:'+lg.error_code)
print('login respond  error_msg:'+lg.error_msg)

# 获取沪深300成分股
rs = bs.query_hs300_stocks()
print('query_hs300 error_code:'+rs.error_code)
print('query_hs300  error_msg:'+rs.error_msg)

# 打印结果集
hs300_stocks = []
i=0
while (rs.error_code == '0') & rs.next():
    # 获取一条记录，将记录合并在一起
    data_item = rs.get_row_data()
    updateDate = data_item[0]
    code = data_item[1]
    code_name = data_item[2]
    hs300_stocks.append(data_item)
    i+=1
    print(i, ' ', rs.get_row_data())
    sql = f"INSERT INTO hs300name (code, name, update_date) VALUES " \
          f"('{code}', '{code_name}','{updateDate}')"
    cursor.execute(sql)


result = pd.DataFrame(hs300_stocks, columns=rs.fields)
# 结果集输出到csv文件
result.to_csv("/home/wieneralan/software/quant/hs300_stocks.csv", encoding="gbk", index=False)
print(result)

# 登出系统
bs.logout()

# 提交更改  # 关闭游标和连接
conn.commit()
cursor.close()
conn.close()


