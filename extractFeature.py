from utils.SQLutils import connectMysql, readMysqlTable2DF, MysqlEngine
import utils.featureUtils as featureUtils
import pandas as pd
import numpy as np

# 连接Mysql的引擎
sql_engine = MysqlEngine(host='localhost',port='3306',user='wieneralan',password='0922',database='stock_data')

## 从Mysql的中读数据
symbols = readMysqlTable2DF(sql_engine,'hs300name_wiki', sel_column='code') #读取股票名和id
df = readMysqlTable2DF(sql_engine,'hs300_daily_price', sel_column='*') # 读取对应的股票每日价格等
print('hs300 shape ',df.shape)

# 定义特征数据列表
df_feat = pd.DataFrame()                   
# 循环提取特征
num = 0
for index, symbol in symbols['code'].items():
    num+=1
    # if(num>2):
    #     break
    # 依次取出每个股票的数据
    iter_feat = pd.DataFrame() # 初始化每次的数据列表
    df_symbol=df.query('symbol == @symbol')
    # 增加股票id和日期列
    iter_feat['symbol'] = df_symbol['symbol'] # 存股票id
    iter_feat['price_date'] = df_symbol['price_date'] # 存股票日期
    iter_feat['close_price'] = df_symbol['close_price'] # 存待预测的值

    # 计算特征
    
    # iter_feat['open_price_daily_return'] = featureUtils.create_open_price_return(df_symbol['open_price']) # 开盘价涨幅比例
    iter_feat['high_price_daily_return'] = featureUtils.create_history_return(df_symbol['high_price']) # 昨天最高价涨幅
    iter_feat['low_price_daily_return'] = featureUtils.create_history_return(df_symbol['low_price']) # 昨天最低价涨幅
    iter_feat['amount_daily_return'] = featureUtils.create_history_return(df_symbol['amount']) # 昨天成交量涨幅
    # iter_feat['open_price_return_n'] = featureUtils.create_open_price_return_n(df_symbol['open_price'],df_symbol['high_price'],df_symbol['low_price'],5) # 今日相比昨日开盘的涨幅/过去n天最高价与最低价的差值
    iter_feat['EMV'] = featureUtils.create_EMV(df_symbol['high_price'], df_symbol['low_price'], df_symbol['amount'],14)

    iter_feat['SMA'] = featureUtils.create_SMA(df_symbol['close_price'], 30)
    iter_feat['TRIMA'] = featureUtils.create_TRIMA(df_symbol['close_price'], 30)
    iter_feat['WMA'] = featureUtils.create_WMA(df_symbol['close_price'], 30)
    iter_feat['ROC'] = featureUtils.create_ROC(df_symbol['close_price'], 10)
    iter_feat['ROCP'] = featureUtils.create_ROCP(df_symbol['close_price'], 10)
    iter_feat['CCI'] = featureUtils.create_CCI(df_symbol['high_price'],df_symbol['low_price'],df_symbol['close_price'],20)
    iter_feat['Force_Index'] = featureUtils.create_Force_Index(df_symbol['close_price'], df_symbol['amount'], 1)
    iter_feat['BBupper'],iter_feat['middle2'],iter_feat['lower2'] = featureUtils.create_BBANDS(df_symbol['close_price'])
    
    # 存入DataFrame中
    df_feat = pd.concat([df_feat, iter_feat])

    print(num, 'df_feat',df_feat.shape, 'iter_feat',iter_feat.shape, type(df_feat), type(iter_feat))
print(df_feat)

df_feat.insert(0,'id', range(1,len(df_feat)+1)) ## 添加id行
df_feat=df_feat.replace([np.inf, -np.inf], np.nan) # 将空值进行替换，否则存入Mysql时会报错

# 将数据存入Mysql中，如果表名不存在 会自动创建
df_feat.to_sql('hs300_feat',con=sql_engine,index=False,if_exists='replace')


# print(df_feat.head(30))
# print(df_feat)#.iloc[0:9050])

# print(df_feat.shape)#.iloc[0:9050])




