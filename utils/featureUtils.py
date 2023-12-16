import numpy as np
import pandas as pd
import talib

def create_lagged_series(ts, start_date, end_date, lags=5):
    """
    This creates a pandas DataFrame that stores the 
    percentage returns of the adjusted closing value of 
    a stock obtained from Yahoo Finance, along with a 
    number of lagged returns from the prior trading days 
    (lags defaults to 5 days). Trading volume, as well as 
    the Direction from the previous day, are also included.
    """

    # Obtain stock information from Yahoo Finance
    # ts = DataReader(
    #     symbol, "yahoo", 
    #     start_date-datetime.timedelta(days=365), 
    #     end_date
    # )

    ts = ts.query('@start_date <= price_date <= @end_date')
    ts.set_index('price_date', inplace=True)
    # Create the new lagged DataFrame
    tslag = pd.DataFrame(index=ts.index)

    tslag["Today"] = ts["close_price"]
    tslag["Volume"] = ts["amount"]

    # Create the shifted lag series of prior trading period close values
    for i in range(0, lags):
        tslag["Lag%s" % str(i+1)] = ts["close_price"].shift(i+1)

    # Create the returns DataFrame
    tsret = pd.DataFrame(index=tslag.index)
    tsret["Volume"] = tslag["Volume"]
    tsret["Today"] = tslag["Today"].pct_change()*100.0

    # If any of the values of percentage returns equal zero, set them to
    # a small number (stops issues with QDA model in scikit-learn)
    for i,x in enumerate(tsret["Today"]):
        if (abs(x) < 0.0001):
            tsret["Today"][i] = 0.0001

    # Create the lagged percentage returns columns
    for i in range(0, lags):
        tsret["Lag%s" % str(i+1)] = \
        tslag["Lag%s" % str(i+1)].pct_change()*100.0

    # Create the "Direction" column (+1 or -1) indicating an up/down day
    tsret["Direction"] = np.sign(tsret["Today"])
    tsret = tsret[tsret.index >= start_date]

    return tsret


def create_open_price_return(df):
    # 今天相对昨天的涨幅比，res = (df[i]-df[i-1])/df[i-1]
    feat = df.pct_change() # 开盘价涨幅比
    return feat

def create_history_return(df):
    # 昨日涨幅比，res = (df[i-1]-df[i-2])/df[i-2]
    feat = df.pct_change().shift(1) # 昨天最高价涨幅比
    return feat

def create_open_price_return_n(df_open,df_high,df_low,N):
    """
    输入:开盘价、最高价、最低价、窗口大小过去N天
    今日相比昨日开盘的涨幅/过去n天最高价与最低价的差值, 过去2天是指昨天和前天
    计算时不用到今日值
    res = (df_open[i]- df_open[i-1])/(max(df_high[i-N:i-1]) - min(df_low[i-N:i-1]))
    """
    iter_feat_high_price_max_n = df_high.rolling(window=N).max().shift(1) # 过去n天的最高价，
    feat_low_price_min_n = df_low.rolling(window=N).min().shift(1) # 过去n天的最低价

    feat_high_low_range = df_high.rolling(window=N).max().shift(1) \
                                  - df_low.rolling(window=N).min().shift(1) # 过去n天的振幅，即最高价与最低价之差
    feat_open_price_range = df_open-df_open.shift(1) # 今日相比昨日开盘的涨幅
    feat_open_price_range_percent = feat_open_price_range/feat_high_low_range # 相对波动=今日相比昨日开盘的涨幅/过去n天最高价与最低价的差值
    return feat_open_price_range_percent

def create_SMA(df, N=30):
    """
    收盘价:N天滑动平均, 计算时用到今日的值
    res[i] = mean(df[i-N-1:i])
    """
    feat = talib.SMA(df,timeperiod=N)
    # feat= df.rolling(window=30).mean()
    return feat

def create_TEMA(df, N=30):
    """
    Triple Exponential Moving Average  指数移动平均EMA的一种变体
    """
    feat = talib.TEMA(df,timeperiod=N)
    return feat

def create_TRIMA(df, N=30):
    """
    Triple Exponential Moving Average  指数移动平均EMA的一种变体
    """
    feat = talib.TRIMA(df,timeperiod=N)
    return feat

def create_TRIMA(df, N=30):
    """
    Triangular Moving Average
    """
    feat = talib.TRIMA(df,timeperiod=N)
    return feat

def create_WMA(df, N=30):
    """
    Weighted Moving Average 
    """
    feat = talib.WMA(df,timeperiod=N)
    return feat

def create_ROC(df, N=10):
    """
    ((real/prevPrice)-1)*100 (Momentum Indicators)
    """
    feat = talib.ROC(df,timeperiod=N)
    return feat

def create_ROCP(df, N=10):
    """
    ((real/prevPrice)-1)*100 (Momentum Indicators)
    """
    feat = talib.ROCP(df,timeperiod=N)
    return feat

def create_CCI(df_high,df_low,df_close, N):
    """
    顺势指标，是测量股价是否已超出常态分布范围的一个指数，
    一般与 100 和 -100 比较。100 以上为超买，表示价格虚高，
    -100 以下为超卖，表示价格过低。
    其计算方法为： 
    CCI = (Typical price - MA of Typical price) / (0.015 * Standard deviation of Typical price)
    """
    feat = talib.CCI(df_high, df_low, df_close, timeperiod=N)
    return feat

def create_EMV(df_high,df_low,df_amount, N):
    """
    """
    EM = ( (df_high + df_low) / 2 - (df_high.shift(1) + df_low.shift(1)) / 2 ) \
            * (df_high - df_low) / df_amount
    feat = talib.SMA(EM,timeperiod=N)
    return feat

def create_BBANDS(df):
    feat_BBupper, feat_middle, feat_lower = talib.BBANDS(df, matype=talib.MA_Type.T3)
    return feat_BBupper, feat_middle, feat_lower

def create_Force_Index(df, df_amount,N):
    feat = (df.diff(N)) * df_amount
    return feat