import pandas as pd  
import talib

# 创建两个Series  
df_feat = pd.DataFrame(columns=['a'])
s1 = pd.Series([1, 2, 3])  
s2 = pd.Series([4, 5])  

print('s1--')
print(s1[1:3])
print('---end')
# df_feat['a']=s2
# 使用concat函数拼接Series  
for i in range(3):
    # print(i)
    df_feat= pd.concat([df_feat, s2])  

print(df_feat)
print(df_feat.pct_change(),s2.shape)


# 创建一个示例DataFrame
data = {'A': [1, 3, 6, 10, 15], 'B': [2, 4, 7, 11, 16]}
df = pd.DataFrame(data)

# 计算一阶差分
diff_result = df.diff(2)
diff_ROC = diff_result/df.shift(2)
diff_ta = talib.ROCP(df['B'],timeperiod=2)

a = (df['B'] - df['B'].shift(2))


# # 打印结果
# print(diff_result)

# print(a)
import datetime  
  
# 获取当前日期  
today = datetime.date.today()  

date_string = "2023-08-01"
date = datetime.date.fromisoformat(date_string)
# 生成一周的日期列表  
dates = [date + datetime.timedelta(days=i) for i in range(7)]  
  
# 打印日期列表  
for date in dates:  
    print(date)



import pandas as pd

# 创建一个示例 DataFrame
data = {'A': [1, 2, 3, 4, 5]}
df = pd.DataFrame(data)

# 定义要移动的行数
shift_amount = 2

# 使用 shift 方法移动数据，并在尾部插入NaN
df['A_shifted'] = df['A'].shift(-shift_amount)

# 在头部插入NaN
df.loc[:shift_amount - 1, 'A_shifted'] = None

print(df)


df = pd.Series([1,2, 3, 4])

new_data = [5,6,7,8]
# df.tolist()
df_new = pd.Series( new_data)
print(df)
print(df_new,type(df_new))
# df_new = pd.DataFrame([df,df_new])
df_new = pd.concat([df, df_new], axis=1)  
print(df_new,type(df_new))


df = pd.DataFrame(columns=["A","B","C"]) # 初始化数据列表
df = pd.DataFrame({'A': ['Alice', 'Bob'], 'B': [25, 30],'C': [2, 3]})  
print(df)
df2 = pd.DataFrame(columns=["A","B","C"]) # 初始化数据列表

df2.loc[len(df2)] = [1,2,3]
df2.loc[len(df2)] = [4,5,6]
print(df2, type(df2))
df = pd.concat([df,df2],axis=0)
print(df)



# df4 = pd.DataFrame({'A': ['Alice', 'Bob'], 'B': [25, 30],'C': [2, 3]})
# print(df4)

# data = [['Alice', 25], ['Bob', 30], ['Charlie', 35]] 
# data2 = [['Alice', 25]]
# df = pd.DataFrame(data2, columns=['name', 'age'])
# print(df)

from utils import SQLutils

sql_engine = SQLutils.MysqlEngine(host='localhost',port='3306',user='wieneralan',password='0922',database='stock_data')
df_feat = SQLutils.Mysql2DF(sql_conn=sql_engine, symbol='601318',table_name='hs300_feat',sel_column='*',start_date='2010-01-01', end_date='2023-11-27') # 取出特征
df_price = SQLutils.Mysql2DF(sql_conn=sql_engine, symbol='601318',table_name='hs300_daily_price',sel_column='close_price',start_date='2010-01-01', end_date='2023-11-27') # 从数据库取出收盘价

df_price['range'] = df_price['close_price']-df_price['close_price'].shift(1)
data = df_price['range']
data[data>=0]=1
data[data<0]=-1
df_price['y_data']=data
print(type(data))
print(df_price)