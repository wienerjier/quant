
"""
分析predict.py预测的结果,与真实值比较，并画图
"""
import akshare as ak
import datetime
from utils.SQLutils import MysqlEngine, readMysqlTable2DF
from utils.SQLutils import MysqlEngine, readMysqlTable2DF

import matplotlib.pyplot as plt
import pandas as pd

#设置开始日期， 设置结束日期为今天
start_date = "20230101"
today = datetime.date.today()
today = today.strftime("%Y%m%d") # 转化成Akshare的日期形式
# today = "20231206"

# 取出真实数据
stock_id = "601318"
df_symbol = ak.stock_zh_a_hist(symbol=stock_id, period="daily", start_date=start_date, end_date=today, adjust="")
print(df_symbol)


## 因为预测的是30天后的数据，所以这里制作一个30天的临时表，后续将其插入到预测值表中
columns_name = ["ElasticNet_model.pkl","Ridge_model.pkl","LinearRegression_model.pkl","RandomForestRegressor_model.pkl"]
df_tmp = pd.DataFrame(columns = columns_name)
a=[45]*4
for i in range(30):
    df_tmp = pd.concat([df_tmp,pd.DataFrame([a],columns = columns_name)]                    )

# 从sql中读取预测的结果
sql_engine = MysqlEngine(host='localhost',port='3306',user='wieneralan',password='0922',database='stock_data')# 连接Mysql的引擎
df = readMysqlTable2DF(sql_engine,'predict_601318',sel_column='*')## 从Mysql的中读数据
# 获取真实结果
df_true = df_symbol['开盘']
print(df_true)

# 真实结果与预测结果一一对应起来
df = pd.concat([df_tmp,df],axis=0).reset_index(drop=True)  # 预测结果后移30天，将 index设置为从0开始
print('df',df)
df_true = pd.Series(df_true.tolist()+[45]*30, name='开盘')# 真实值尾部加入30天，与
print(df_true)
# plt.plot(df_symbol['收盘'].values,'r')
# df_symbol['收盘'].plot()

df['true'] = df_true
df = df.drop(['ElasticNet_model.pkl','Ridge_model.pkl','RandomForestRegressor_model.pkl'],axis=1) # 舍弃对应的列
# print(df)
df.plot()
plt.show()