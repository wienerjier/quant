import mysql.connector
import pandas as pd
from sqlalchemy import create_engine

def connectMysql(host='localhost',user='wieneralan',password='0922',database='stock_data'):
    """
    连接Mysql服务器
    localhost: Mysql地址
    user: Mysql 用户名
    password: 用户名密码
    database: 要访问的数据库
    """
    # 连接到MySQL数据库
    conn = mysql.connector.connect(
        host='localhost',
        user='wieneralan',
        password='0922',
        database='stock_data'
    )    
    return conn

def MysqlEngine(host='localhost',port='3306',user='wieneralan',password='0922',database='stock_data'):
    """
    连接Mysql服务器
    localhost: Mysql地址
    user: Mysql 用户名
    password: 用户名密码
    database: 要访问的数据库
    """
    # 连接到MySQL数据库
    # con = f'mysql://wieneralan:0922@localhost:3306/stock_data?charset=utf8'
    engine = create_engine(f'mysql://{user}:{password}@{host}:3306/{database}?charset=utf8') # utf8是为了解决中文问题
    return engine

def readMysqlTable(sql_conn, table_name, sel_column):
    """
    sql_conn:连接到的sql对象，这里是connectMysql()函数的返回值
    table_name:字符串,要读取的Mysql的数据库的Table表
    sel_colum:字符串，要选取Table_name中的列名，默认全选'*'，选择部分列名之间用空格，'id code name'
    返回： list
    """
    # 创建游标
    cursor = sql_conn.cursor()
    # 执行查询语句
    query = f'SELECT {sel_column} FROM {table_name}'
    cursor.execute(query)
    # 获取查询结果
    result = cursor.fetchall() ## result is list
    # 关闭游标和连接
    cursor.close()

    return result

def readMysqlTable2DF(sql_conn, table_name, symbol=None, sel_column='*'):
    """
    sql_conn:连接到的sql对象，这里是MysqlEngine()函数的返回值
    table_name:字符串,要读取的Mysql的数据库的Table表
    sel_colum:字符串，要选取Table_name中的列名，默认全选'*'，选择部分列名之间用逗号，'id,code,name'
    返回值：pandas.core.frame.DataFrame
    """
    # 执行查询语句
    if(symbol is None):
        query = f'SELECT {sel_column} FROM {table_name}'
    else:
        query = f'SELECT {sel_column} FROM {table_name} WHERE symbol={symbol}'

    result = pd.read_sql(query, sql_conn)

    return result

def Mysql2DF(sql_conn=None, symbol='', table_name='', sel_column='*', start_date=None, end_date=None):
    """
    从数据表table_name中读symbol对应的,在start_date和end_date之间的数据
    symbol:股票id
    sql_conn:连接到的sql对象,这里是MysqlEngine()函数的返回值
    table_name:字符串,要读取的Mysql的数据库的Table表
    sel_colum:字符串, 要选取Table_name中的列名,默认全选'*'，选择部分列名之间用逗号，'id,code,name'
    start_date: 单引号字符串 开始读取的日期,比如：'2023-04-18'
    end_date: 结束读取的日期
    返回值: pandas.core.frame.DataFrame
    """
    # 执行查询语句    
    if(start_date is not None):
        if(end_date is not None): # 开始结束日期都不空
            query = f'SELECT {sel_column} FROM {table_name} WHERE symbol={symbol} \
                    AND price_date >= "{start_date}" AND price_date <= "{end_date}" '   ## 注意单引号里面， start_date,end_date这里要加双引号
        else: # 结束日期为None
            query = f'SELECT {sel_column} FROM {table_name} WHERE symbol={symbol} \
                    AND price_date >= "{start_date}" '   
    else:# 开始日期None
        if(end_date is not None):
            query = f'SELECT {sel_column} FROM {table_name} WHERE symbol={symbol} \
                    AND price_date <= "{end_date}" '   
        else: # 都是None时，全选
            query = f'SELECT {sel_column} FROM {table_name} WHERE symbol={symbol}'

    result = pd.read_sql(query, sql_conn)
    return result