import akshare as ak
import datetime
import utils.featureUtils as featureUtils
import pandas as pd
from utils.SQLutils import Mysql2DF, MysqlEngine
from sklearn import preprocessing
import pickle
import joblib 


def acquire_feat(stock_id, start_date, predict_date):
    """从AKshare获取指定股票id的数据, 并提取特征，
       注意这里提取特征时,要与train.py(extractFeature.py)中使用的特征要一致
    """
    #设置开始日期， 设置结束日期为今天
    df_symbol = ak.stock_zh_a_hist(symbol=stock_id, period="daily", start_date=start_date, end_date=predict_date, adjust="")
    # 提取特征
    iter_feat = pd.DataFrame() # 初始化每次的数据列表
    # iter_feat['open_price_daily_return'] = featureUtils.create_open_price_return(df_symbol['开盘']) # 开盘价涨幅比例
    iter_feat['high_price_daily_return'] = featureUtils.create_history_return(df_symbol['最高']) # 昨天最高价涨幅
    iter_feat['low_price_daily_return'] = featureUtils.create_history_return(df_symbol['最低']) # 昨天最低价涨幅
    iter_feat['amount_daily_return'] = featureUtils.create_history_return(df_symbol['成交量']) # 昨天成交量涨幅
    # iter_feat['open_price_return_n'] = featureUtils.create_open_price_return_n(df_symbol['开盘'],df_symbol['最高'],df_symbol['最低'],5) # 今日相比昨日开盘的涨幅/过去n天最高价与最低价的差值
    iter_feat['EMV'] = featureUtils.create_EMV(df_symbol['最高'], df_symbol['最低'], df_symbol['成交量'],14)

    iter_feat['SMA'] = featureUtils.create_SMA(df_symbol['收盘'], 30)
    iter_feat['TRIMA'] = featureUtils.create_TRIMA(df_symbol['收盘'], 30)
    iter_feat['WMA'] = featureUtils.create_WMA(df_symbol['收盘'], 30)
    iter_feat['ROC'] = featureUtils.create_ROC(df_symbol['收盘'], 10)
    iter_feat['ROCP'] = featureUtils.create_ROCP(df_symbol['收盘'], 10)
    iter_feat['CCI'] = featureUtils.create_CCI(df_symbol['最高'],df_symbol['最低'],df_symbol['收盘'],20)
    iter_feat['Force_Index'] = featureUtils.create_Force_Index(df_symbol['收盘'], df_symbol['成交量'], 1)
    iter_feat['BBupper'],iter_feat['middle2'],iter_feat['lower2'] = featureUtils.create_BBANDS(df_symbol['收盘'])
    
    # print('iterfeat:********',iter_feat)
    # iter_feat['SMA'] = featureUtils.create_SMA(df_symbol['收盘'], 30)
    #-- 取出要预测predict_date当天对应的特征
    X_yesterday = iter_feat[-2:-1]
    X_today = X_yesterday #iter_feat[-1:]
    # 使用训练数据的normalize模型进行，数据标准化
    scaler = joblib.load('./models/scaler.pkl')  # 从文件中加载标准化模型
    X_today_norm = scaler.transform(X_today)

    return X_today_norm

def predict_next(X,model_name):
    """输入某天股票的特征，和训练好的模型，输出预测结果
       model_name是模型列表
    """
    y_tomorrow = pd.DataFrame(columns=model_name) # 初始化每次的数据列表
    tomorrow =[]
    for m in model_name:
        with open("./models/"+m, 'rb') as file:
            loaded_model = pickle.load(file)

        y_pred = loaded_model.predict(X)
        tomorrow.append(y_pred[0])
        # print(y_pred[0])
    y_tomorrow.loc[len(y_tomorrow)] = tomorrow
    # print(y_tomorrow)
    return y_tomorrow


if __name__ == "__main__":
    stock_id = "601318"
    start_date = "20221101"
    # predict_date = datetime.date.today().strftime("%Y%m%d") # 转化成Akshare的日期形式
    predict_date_start = "20230101"
    predict_date_end = "20231206"
    model_name = ["ElasticNet_model.pkl","Ridge_model.pkl",
                    "LinearRegression_model.pkl","RandomForestRegressor_model.pkl"]
    # 获取要预测的日期 数据
    df_symbol = ak.stock_zh_a_hist(symbol=stock_id, period="daily", start_date=predict_date_start, end_date=predict_date_end, adjust="")
    print(df_symbol)
    # 将待预测的日期，转化成Akshare的日期形式
    dates=[x.strftime("%Y%m%d") for x in df_symbol["日期"]]
    print(dates,len(dates))
    # 依次预测每天的值，并存入mysql中
    for date in dates:
        print(date)
        X_today_norm = acquire_feat(stock_id=stock_id, start_date=start_date, predict_date=date)
        # print(X_today_norm)
        y_tomorrow = predict_next(X=X_today_norm, model_name=model_name)

        sql_engine = MysqlEngine(host='localhost',port='3306',user='wieneralan',password='0922',database='stock_data')
        y_tomorrow.to_sql("predict_"+stock_id, con=sql_engine,index=False,if_exists='append')
