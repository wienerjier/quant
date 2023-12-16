"""
从Mysql:stock_data-hs300name_wiki中读出沪深300支股票的编号, 
通过AKShare读取历史价格,存入Mysql-hs300_daily_price数据库中
"""

from utils import connectMysql, readMysqlTable
from sqlalchemy import create_engine
import akshare as ak
import datetime

now = datetime.date.today()

if __name__ == "__main__":
    # 连接数据库
    conn = connectMysql(host='localhost',user='wieneralan',password='0922',database='stock_data')
    # 从Mysql中取出沪深300的股票id
    result = readMysqlTable(conn,'hs300name_wiki', 'code,name')
    # print(result)
    # 创建pandas存入Mysql用到的引擎
    engine = create_engine('mysql://wieneralan:0922@localhost:3306/stock_data?charset=utf8') # utf8是为了解决中文问题

    i=0
    for row in result:
        i+=1
        # 依次拉取每个股票的历史数据，start_date=“”代表从最早的日期开始读
        stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol=row[0], period="daily", start_date="", end_date='20231127', adjust="hfq")
        print(i,row, '开始日期：',stock_zh_a_hist_df.iloc[0]['日期'])
        # 增加股票id、股票名字、数据库创建日期
        stock_zh_a_hist_df.insert(0,'symbol',row[0])
        stock_zh_a_hist_df.insert(1,'name',row[1])
        stock_zh_a_hist_df['created_date'] = now
        # 修改pandas.DataFrame的列名字 为 Mysql数据库里的列名
        stock_zh_a_hist_df.rename(
            columns={'日期': 'price_date',
                    '开盘': 'open_price',
                    '收盘': 'close_price',
                    '最高': 'high_price',
                    '最低': 'low_price',
                    '成交量': 'amount',
                    '成交额': 'turnover',
                    '振幅': 'amplitude',
                    '涨跌幅': 'chg',
                    '涨跌额': 'change',
                    '换手率': 'TOR'}, 
                    inplace=True)
        # 将DataFrame格式的数据存入Mysql
        stock_zh_a_hist_df.to_sql('hs300_daily_price_hfq',con=engine,index=False,if_exists='append')

    print("write to sql success")
    # print(type(stock_zh_a_hist_df))

