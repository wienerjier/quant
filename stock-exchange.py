"""网上download的一个代码
   根据每支股票的动量EMV和MAEMV指标,设计交易策略:每次买入信号来的时候买100股,每次卖出信号来的时候全部卖出
   当emv和maemv指数均大于0,并且emv指数上穿maemv我们就买入,
   当emv下穿maven咋就卖出。并设定投资金额是5000元人名币,每次笔交易的服务费是0.2%"""


import talib
import matplotlib.pyplot as plt
from matplotlib.pylab import date2num
import matplotlib.ticker as ticker  # 用于日期刻度定制
import baostock as bs
import pandas as pd
import datetime
from matplotlib import colors as mcolors  # 用于颜色转换成渲染时顶点需要的颜色格式
from matplotlib.collections import LineCollection, PolyCollection  # 用于绘制直线集合和多边形集合
from matplotlib.widgets import Cursor  # 处理鼠标

from utils import readMysqlTable2DF, MysqlEngine



def date_to_num(dates):
    num_time = []
    for date in dates:
        date_time = datetime.datetime.strptime(date,'%Y-%m-%d')
        num_date = date2num(date_time)
        num_time.append(num_date)
    return num_time

# 绘制蜡烛图
def format_date(x, pos=None):
    # 日期格式化函数，根据天数索引取出日期值
    return '' if x < 0 or x > len(date_tickers) - 1 else date_tickers[int(x)]

#EMV技术计算
def TEMV(data,fasttimeperiod,lasttimeperiod):
    temp=data
    temp['sub']=2
    emFront=talib.DIV(talib.ADD(temp['high'],temp['low']),temp['sub'])
    emFrontSub=talib.DIV(talib.ADD(temp['high'].shift(1),temp['low'].shift(1)),temp['sub'])
    emEnd=talib.DIV(talib.SUB(temp['high'],temp['low']),temp['volume'])
    em= talib.SUB(emFront,emFrontSub)*emEnd
    EMV=talib.SMA(em,fasttimeperiod)
    MAEMV=talib.SMA(EMV,lasttimeperiod)
    SubEmv=talib.SUB(EMV,MAEMV)
    return EMV,MAEMV,SubEmv

#策略
def BuySallForEmv(Emv,MaEmv):
    """q根据emv和maEmv获取卖出和买入的时间点"""
    BuyIndex=[]
    SallIndex=[]
    temp=pd.DataFrame({ 'emv' :Emv, 'maemv' :MaEmv})
    for index, row in temp.iterrows():
        if row['emv'] is None:
            continue
        #--emv和maemv指数均大于0，并且emv指数上穿maemv我们就买入
        if row['emv'] > 0 and row['maemv']>0 and row['emv']>row['maemv']:
            BuyIndex.append(index)
        # ---emv下穿maven咋就卖出
        elif row['emv']>=0 and row['maemv']>=0 and row['emv']<row['maemv']:
            SallIndex.append(index)
    return BuyIndex,SallIndex


#买股票
def Dobuy(index,price):
    """price:本程序默认使用的收盘价"""
    global totalRmb, handTotal
    global buysell, myRmb, lastRmb
    # 买入信号来的时候买100股，每笔交易的服务费是0.2%
    currentRmb=price*100*1.002 # 每次买入花费的费用
    if totalRmb-currentRmb>0:
        totalRmb=totalRmb-currentRmb  ## 现有现金
        handTotal=handTotal+1  ## 持有的股票数 100为单位
        buysell.append(index-start)  ## 
        myRmb.append(totalRmb+handTotal*100*price)
        print("++++买入： 总金额：" + str(totalRmb) + "   总手数" + str(handTotal)+"   账户总金额："+str(totalRmb+handTotal*100*price))
        lastRmb = totalRmb+handTotal*100*price
    else:
        print("++++买入：资金不足")

#卖股票
def Dosell(index,price):
    """price:本程序默认使用的收盘价"""
    global buysell, myRmb
    global totalRmb, handTotal,lastRmb

    # 每次卖出信号来的时候全部卖出
    if handTotal>0:
        currentRmb=handTotal*100*price*0.998 # 卖出股票获得的金额 
        totalRmb=totalRmb+currentRmb ## 现有现金
        buysell.append(index-start)  # 相对start的偏移，buysell最终是0-359的值
        myRmb.append(totalRmb)  # 因为是全部卖出，现有现金即是账户总金额
        handTotal=0
        print("----卖出： 总金额："+str(totalRmb)+"   总手数"+str(handTotal)+"   账户总金额："+str(totalRmb))
        lastRmb = totalRmb
    else:
        print("----卖出： 不用再往出卖了")

#历史回测
def calculateHistory(doBuy,doSell):
    """doBuy: 买入时刻的股票数据集, doSell:卖出时刻的股票数据集
       wang: result中 买入和卖出时刻的数据集和，并按日期顺序排序
       默认使用收盘价格买入和卖出
       """
    bySort=doBuy.index
    bySell=doSell.index
    doBuy['could']=1
    doBuy['sort']=bySort
    doSell['could']=-1
    doSell['sort']=bySell
    temp=pd.concat([doBuy,doSell])
    # print('history*******',doBuy,doSell)
    ## wang 数据为 买入和卖出时的数据集和，并按实际的日期顺序排序，cloud列 1：买入  -1：卖出
    wang=temp.sort_values(by="sort", ascending=True)
    # print('concat',wang)
    for index, row in wang.iterrows():
        # print("序号 买入1 卖出-1 序号 最低价格: " + str(index)+"  "+str(row['could'])+"  "+str(row['sort'])+"   "+str(row['low']))
        print("序号 买入1 卖出-1 序号 最低价格: " + str(index)+"  "+str(row['could'])+"  "+str(row['sort'])+"   "+str(row['close']))
        if row['could']==1:
            Dobuy(index,float(row['close']))
        elif row['could']==-1:
            price=float(row['close'])
            Dosell(index,price)



# 连接Mysql的引擎
sql_engine = MysqlEngine(host='localhost',port='3306',user='wieneralan',password='0922',database='stock_data')
## 从Mysql的中读数据
df = readMysqlTable2DF(sql_engine,'hs300name_wiki', sel_column='code,exchange_id,name')
final_money = pd.DataFrame(columns=["stock_id","name","cash"]) # 初始化每次的数据列表
lg = bs.login()
for index, symbol in df.iterrows():
    lastRmb = 0 #---最后账户总金额
    totalRmb=20000  ## 现有的现金
    handTotal=0  ## 现有持股数目，100为单位
    buysell=[]
    myRmb=[]  # 每次买入后的账户总金额 = 现有现金+持有的股票价值
    print("index:", index)
    # print(lastRmb, totalRmb, handTotal)
    # print(symbol)
    stock_id = symbol[1]+'.'+symbol[0] 
    # print(stock_id)
    # stock_id = symbols['exchange_id'] +'.' + symbols['code']
    # sh.6010567 设置bs爬虫连接"sh.601318"
    # if(stock_id=="sz.000425" or stock_id=="sz.000983"):
    #     continue
    rs = bs.query_history_k_data_plus(stock_id,
        "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST",
        start_date='2017-07-01', end_date='2021-12-31',
        frequency="d", adjustflag="3")

    #### 获取股票数据， 打印结果集 ####
    data_list = []
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())
    result = pd.DataFrame(data_list, columns=rs.fields)
    if(result.empty):
        continue
    # print('result:::',result)
    window=360
    start=len(result)-window  ## 最近window=360天 开始的index
    #二维数组， 取最近360天的数据
    result=result.loc[:,['date','open','high','low','close','volume'] ]
    # print('result:::',result)
    result=result[-window:] # 
    # print('result:::',result)

    date_tickers=result.date.values
    result.date = range(0, len(result))  # 日期改变成序号
    matix = result.values  # 转换成绘制蜡烛图需要的数据格式(date, open, close, high, low, volume)
    xdates = matix[:,0] # X轴数据(这里用的天数索引)
    #总投资金额为5000元，买入信号出现时每次买一手。如果有卖出信号则全部卖出
    # 提取指标
    t3Price = talib.T3(result['close'], timeperiod=10, vfactor=0) #Triple Exponential Moving Average (T3)
    Adxprice = talib.ADX(result['high'],result['low'],result['close'], timeperiod=5)
    Adxrprice = talib.ADXR(result['high'],result['low'],result['close'], timeperiod=5)
    try:
        emv,maemv,subemv=TEMV(result,14,10)
    except Exception as e:
        print(e)
        continue
    
    buy,sell=BuySallForEmv(emv,maemv)  ## buy: 买入、卖出 时对应 result中的index
    # print('buy and sell ', buy, sell)
    realBuy=result.index.isin(buy)  ## realBuy：bool类型， 标记每个index是否买入
    doBuy=result[realBuy]  ## 买入时， 对应的reslut数据
    realSell=result.index.isin(sell)
    doSell=result[realSell]   ## 卖出时， 对应的reslut数据
    # print('realbuy and sell ', realBuy, realSell)
    # print('dobuy and sell ', doBuy, doSell)
    if(doBuy.empty and doSell.empty):
        continue

    upperband, middleband, lowerband = talib.BBANDS(result['close'], timeperiod=5, nbdevup=2, nbdevdn=2, matype=0)
    # 设置外观效果
    # plt.rc('font',family='NSimSun')  # 用中文字体，防止中文显示不出来
    plt.rc('figure', fc='k')  # 绘图对象背景图
    plt.rc('text', c='#800000')  # 文本颜色
    plt.rc('axes', axisbelow=True, xmargin=0, fc='k', ec='#800000', lw=1.5, labelcolor='#800000',
        unicode_minus=False)  # 坐标轴属性(置底，左边无空隙，背景色，边框色，线宽，文本颜色，中文负号修正)
    plt.rc('xtick', c='#d43221')  # x轴刻度文字颜色
    plt.rc('ytick', c='#d43221')  # y轴刻度文字颜色
    plt.rc('grid', c='#800000', alpha=0.9, ls=':', lw=0.8)  # 网格属性(颜色，透明值，线条样式，线宽)
    plt.rc('lines', lw=0.8)  # 全局线宽

    # 创建绘图对象和4个坐标轴
    fig = plt.figure(figsize=(16, 8))
    left, width = 0.05, 0.9
    ax1 = fig.add_axes([left, 0.6, width, 0.4])  # left, bottom, width, height
    ax2 = fig.add_axes([left, 0.5, width, 0.15], sharex=ax1)  # 共享ax1轴
    ax3 = fig.add_axes([left, 0.35, width, 0.15], sharex=ax1)  # 共享ax1轴
    ax4 = fig.add_axes([left, 0.2, width, 0.15], sharex=ax1)  # 共享ax1轴
    ax5 = fig.add_axes([left, 0.05, width, 0.15], sharex=ax1)  # 共享ax1轴
    plt.setp(ax1.get_xticklabels(), visible=False)  # 使x轴刻度文本不可见，因为共享，不需要显示
    plt.setp(ax2.get_xticklabels(), visible=False)  # 使x轴刻度文本不可见，因为共享，不需要显示
    plt.setp(ax3.get_xticklabels(), visible=False)  # 使x轴刻度文本不可见，因为共享，不需要显示

    ax1.xaxis.set_major_formatter(ticker.FuncFormatter(format_date))  # 设置自定义x轴格式化日期函数
    ax1.xaxis.set_major_locator(ticker.MultipleLocator(max(int(len(result) / 15), 5)))  # 横向最多排15个左右的日期，最少5个，防止日期太拥挤
    # # 下面这一段代码，替换了上面注释的这个函数，因为上面的这个函数达不到同花顺的效果
    opens, closes, highs, lows = matix[:, 1], matix[:, 4], matix[:, 2], matix[:, 3]  # 取出ochl值
    avg_dist_between_points = (xdates[-1] - xdates[0]) / float(len(xdates))  # 计算每个日期之间的距离
    delta = avg_dist_between_points / 4.0  # 用于K线实体(矩形)的偏移坐标计算
    barVerts = [((date - delta, open), (date - delta, close), (date + delta, close), (date + delta, open)) for date, open, close in zip(xdates, opens, closes)]  # 生成K线实体(矩形)的4个顶点坐标
    rangeSegLow = [((date, low), (date, min(open, close))) for date, low, open, close in  zip(xdates, lows, opens, closes)]  # 生成下影线顶点列表
    rangeSegHigh = [((date, high), (date, max(open, close))) for date, high, open, close in zip(xdates, highs, opens, closes)]  # 生成上影线顶点列表
    rangeSegments = rangeSegLow + rangeSegHigh  # 上下影线顶点列表
    # print(rangeSegments)
    cmap = {
            True: mcolors.to_rgba('#000000', 1.0),
            False: mcolors.to_rgba('#54fcfc', 1.0)
    }  # K线实体(矩形)中间的背景色(True是上涨颜色，False是下跌颜色)
    inner_colors = [cmap[opn < cls] for opn, cls in zip(opens, closes)]  # K线实体(矩形)中间的背景色列表
    cmap = {True: mcolors.to_rgba('#ff3232', 1.0),
            False: mcolors.to_rgba('#54fcfc', 1.0)}  # K线实体(矩形)边框线颜色(上下影线和后面的成交量颜色也共用)
    updown_colors = [cmap[opn < cls] for opn, cls in zip(opens, closes)]  # K线实体(矩形)边框线颜色(上下影线和后面的成交量颜色也共用)列表
    #
    ax1.add_collection(LineCollection(rangeSegments, colors=updown_colors, linewidths=0.5,antialiaseds=False))
    # 生成上下影线的顶点数据(颜色，线宽，反锯齿，反锯齿关闭好像没效果)
    ax1.add_collection(PolyCollection(barVerts, facecolors=inner_colors, edgecolors=updown_colors, antialiaseds=False,linewidths=0.5))
    # 生成多边形(矩形)顶点数据(背景填充色，边框色，反锯齿，线宽)

    # 绘制均线
    mav_colors = ['#ffffff', '#d4ff07', '#ff80ff', '#00e600', '#02e2f4', '#ffffb9', '#2a6848']  # 均线循环颜色
    mav_period = [5, 10, 20, 30, 60, 120, 180]  # 定义要绘制的均线周期，可增减
    # mav_period = [5]  # 定义要绘制的均线周期，可增减
    n = len(result)
    for i in range(len(mav_period)):
        if n >= mav_period[i]:
            mav_vals = result['close'].rolling(mav_period[i]).mean().values
            ax1.plot(xdates, mav_vals, c=mav_colors[i % len(mav_colors)], label='MA' + str(mav_period[i]))
    for index, row in doBuy.iterrows():
        ax1.scatter(index-start, float(row['close']), color="y", marker="*")
    for index, row in doSell.iterrows():
        ax1.scatter(index-start, float(row['close']), color="b", marker="^")

    calculateHistory(doBuy,doSell)
    ax1.plot(xdates,t3Price,label='t3price')
    ax1.set_title('sz.002918')  # 标题
    ax1.grid(True)  # 画网格
    ax1.legend(loc='upper left')  # 图例放置于右上角
    ax1.xaxis_date()  # 好像要不要效果一样？

    barVerts = [((date - delta, 0), (date - delta, vol), (date + delta, vol), (date + delta, 0)) for date, vol in zip(xdates, matix[:, 5])]
    # 生成K线实体(矩形)的4个顶点坐标
    try:
        ax2.add_collection(PolyCollection(barVerts, facecolors=inner_colors, edgecolors=updown_colors, antialiaseds=False,linewidths=0.5))
    except Exception as e:
        print('画图错误',e)
        continue
    # 生成多边形(矩形)顶点数据(背景填充色，边框色，反锯齿，线宽)
    if n >= 5:  # 5日均线，作法类似前面的均线
        vol5 = result['volume'].rolling(5).mean().values
        ax2.plot(xdates, vol5, c='y', label='VOL5')
    if n >= 10:  # 10日均线，作法类似前面的均线
        vol10 = result['volume'].rolling(10).mean().values
        ax2.plot(xdates, vol10, c='w', label='VOL10')
    ax2.yaxis.set_ticks_position('right')  # y轴显示在右边
    ax2.legend(loc='upper left')  # 图例放置于右上角
    ax2.grid(True)  # 画网格


    ax3.plot(xdates, Adxprice, c='w', label='Adxprice')
    ax3.plot(xdates, Adxrprice, c='b', label='Adxrprice')
    ax3.legend(loc='upper left')  # 图例放置于右上角
    ax3.grid(True)  # 画网格

    ax4.plot(xdates,emv,c='r',label='EMV')
    ax4.plot(xdates,maemv,c='g',label='MAEMV')
    ax4.axhline(0, ls='-', c='w', lw=0.5)  # 水平线
    ax4.legend(loc='upper left')  # 图例放置于右上角
    ax4.grid(True)  # 画网格

    ax5.axhline(10000, ls='-', c='w', lw=0.5)  # 水平线
    ax5.plot(buysell, myRmb, c='g', label='ge jiu cai 割韭菜')
    ax5.legend(loc='upper left')  # 图例放置于右上角
    ax5.grid(True)  # 画网格

    cursor = Cursor(ax1, useblit=True, color='w', linewidth=0.5, linestyle='--')
    cursor1 = Cursor(ax3, useblit=True, color='w', linewidth=0.5, linestyle='--')
    cursor2 = Cursor(ax4, useblit=True, color='w', linewidth=0.5, linestyle='--')

    final_money.loc[len(final_money)] = [stock_id,symbol[2],lastRmb]
    # print("final_money",final_money)
    print('lastRmb****:',lastRmb)
    # 登出系统
    # plt.show()

bs.logout()

print(final_money)
final_money.to_csv("/home/wieneralan/software/quant/result/hs300_stocks_10000_after1year.csv", encoding="gbk", index=False)
