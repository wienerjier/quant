"""
测试tushare读取数据并存入Mysql中
"""

import tushare as ts
import mysql.connector

# 设置Tushare Token
ts.set_token('f1fc4c6f0efd7120a00983d3016ef86af89e636b18e2de97d9f6cd8d')

# 初始化Tushare接口
pro = ts.pro_api()


# 连接到MySQL数据库
conn = mysql.connector.connect(
    host='localhost',
    user='wieneralan',
    password='0922',
    database='securities_master'
)

# 创建游标
cursor = conn.cursor()

stock_data = pro.daily(ts_code='000001.SZ', start_date='20180701', end_date='20180718')


# 循环遍历股票列表并存储历史交易数据
for index, row in stock_data.iterrows():
    ts_code = row['ts_code']
    trade_date = row['trade_date']
    opendata = row['open']
    high = row['high']
    low = row['low']
    close = row['close']
    pre_close = row['pre_close']
    change = row['change']
    pct_chg = row['pct_chg']
    vol  = row['vol']
    amount = row['amount']

    print('tscode:', ts_code)
    # 获取股票历史交易数据

    # 插入数据到MySQL数据库

    sql = f"INSERT INTO stock_data (ts_code, trade_date, open, high, low, close, volume, amount) VALUES " \
          f"('{ts_code}', '{row['trade_date']}', {row['open']}, {row['high']}, {row['low']}, {row['close']}, " \
          f"{row['vol']}, {row['amount']})"
    cursor.execute(sql)


# 提交更改
conn.commit()

# 关闭游标和连接
cursor.close()
conn.close()


