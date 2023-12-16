from utils.SQLutils import Mysql2DF, MysqlEngine
from sklearn import preprocessing
from sklearn.model_selection import GridSearchCV
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import Ridge, ElasticNet, LinearRegression
from sklearn.metrics import r2_score
import pickle
import joblib
import matplotlib.pyplot as plt

# 连接Mysql的引擎
sql_engine = MysqlEngine(host='localhost',port='3306',user='wieneralan',password='0922',database='stock_data')
## 从Mysql的中读数据

df = Mysql2DF(sql_conn=sql_engine, symbol='601318',table_name='hs300_feat',sel_column='*',start_date='2010-01-01', end_date='2023-11-27') # 取出特征

dp = Mysql2DF(sql_conn=sql_engine, symbol='601318',table_name='hs300_daily_price',sel_column='close_price',start_date='2010-01-01', end_date='2023-11-27') # 从数据库取出收盘价
print(dp)
df['close_price'] = dp.shift(-3)
print(df['close_price'])
# print(df)
# 去除包含None或NaN的行
df_clean = df.dropna()
print('hs300 shape ',df_clean.shape)
# 划分训练集、测试集
train_size = int(df_clean.shape[0]*0.8)
train_data, test_data = df_clean[:train_size], df_clean[train_size:]
# print('train size',train_size)
# print(train_data)
print(test_data)

# # 得到训练集和测试集数据
X_train = train_data.loc[:, ~df_clean.columns.isin(['id','symbol','price_date','close_price'])].copy() # 剔除非特征的列
X_test = test_data.loc[:, ~df_clean.columns.isin(['id','symbol','price_date','close_price'])].copy() # 剔除非特征的列
y_train = train_data['close_price'].copy()
y_test = test_data['close_price'].copy()
# print('X_train',X_train.shape)
print(X_train)
# print('X_test',X_test.shape)
# print(X_test)
# print('y_train',y_train.shape)
# print(y_train)
# print('y_test',type(y_test))
# print(y_test)
# y_train.plot(kind='line')
# y_test.plot(kind='line')
# plt.show()

scaler = preprocessing.StandardScaler().fit(X_train)
joblib.dump(scaler,'./models/scaler.pkl')
X_train_norm = scaler.transform(X_train)
X_test_norm = scaler.transform(X_test)

# print('X_train_norm',X_train_norm.shape)
# print(X_train_norm)
# plt.plot(X_test_norm)
# y_test.plot(kind='line')
# plt.show()


tuned_parameters = [
    {'n_estimatiors':[500,1000], 'min_samples_split':[5], 'min_samples_leaf':[1]}
]
# model = GridSearchCV(RandomForestClassifier(), tuned_parameters, cv=10)
# model.fit(X_train_norm,y_train)

models = [
    ("ElasticNet",ElasticNet()),
    ("Ridge",Ridge()),
    ("LinearRegression",LinearRegression()),
    ("RandomForestRegressor",RandomForestRegressor())
]
for m in models:
    y_train_numpy = y_train.values
    m[1].fit(X_train_norm, y_train_numpy)
    y_pred = m[1].predict(X_test_norm)
    y_pred_end = m[1].predict(X_test_norm[-2:])

    score = r2_score(y_test.values,y_pred)

    with open('./models/'+m[0]+'_model.pkl', 'wb') as file:
        pickle.dump(m[1], file)

    with open('./models/'+m[0]+'_model.pkl', 'rb') as file:
        loaded_model = pickle.load(file)
    y_pred_load = loaded_model.predict(X_test_norm)

    # print(m[0], 'R2 Score', score)
    # print('y_test',type(y_test.values),y_test.values.shape)
    # print(y_test[-2:])
    # print('y_pred',type(y_pred),y_pred.shape)
    # print(y_pred)
    # print(y_pred_end)

    plt.plot(y_test.values,'b')
    plt.plot(y_pred_load,'r')
    plt.show()