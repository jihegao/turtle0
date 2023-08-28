import csv
import numpy as np
from sqlalchemy import create_engine
import psycopg2
import tushare as ts
import pandas as pd
from time import sleep

ts.set_token('cfd0f555b15a532de3dcfb0b910661ca48edca2b5571833144d999df')
pro = ts.pro_api()
engine = create_engine('postgresql+psycopg2://postgres:bcd-199c@localhost:5432/postgres')

def get_code(start='20170101'):
    df = pro.stock_basic(list_status='L')
    codes = df.loc[df.list_date>start]
    return codes['ts_code'].values


# 股票日线
def get_bar(code,start='20170101',end='20220210'):
    df=ts.pro_bar(ts_code=code, adj='hfq', start_date=start, end_date=end)
    return df


def insert_sql(data,db_name,if_exists='append'):
    try:
        data.to_sql(db_name,engine,index=False,if_exists='append')
        print('insert success')
    except:
        pass


# 数据库查询
def get_bar_sql(ts_code):
    df=pd.read_sql("select * from stock_data where ts_code='"+ ts_code + "'" , engine)
    df=df[['trade_date', 'open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_chg', 'vol', 'amount']]
    return np.array(df)


def update_sql(start,end,db_name):
    from datetime import datetime,timedelta
    for code in get_code():
        try:
            data=get_bar(code,start,end)
            insert_sql(data,db_name)
        except:
            pass
    print(f'{start}:{end}期间数据已成功更新')

# 沪深300   
#df = pro.index_daily(ts_code='399300.SZ')
#insert_sql(df,'stock_data')

# update_sql('20180101','20220201','stock_data')


# --------------------------------
# 交易日历
#trade_days = pro.trade_cal(exchange='SSE', start_date='20180101', end_date='20211231')
#trade_days = trade_days.loc[trade_days['is_open']==1, 'cal_date']

# 按交易日下载行情数据并入库
#for d in trade_days:
#    df = pro.daily(trade_date=d)
#    insert_sql(df, 'stock_data')
#    sleep(0.1)