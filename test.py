import csv
from sqlalchemy import create_engine
import psycopg2
import tushare as ts

ts.set_token('cfd0f555b15a532de3dcfb0b910661ca48edca2b5571833144d999df')
pro = ts.pro_api()

engine = create_engine('postgresql+psycopg2://postgres:bcd-199c@localhost:5432/postgres')

def get_data(code,start='20220103',end='20220208'):
    df=ts.pro_bar(ts_code=code, adj='hfq', start_date=start, end_date=end)
    return df

def get_code():
    codes = pro.stock_basic(list_status='L').ts_code.values
    return codes

def insert_sql(data,db_name,if_exists='append'):
    try:
        data.to_sql(db_name,engine,index=False,if_exists=if_exists)
        print(code+'insert success')
    except:
        pass

'''
with open('codes.txt', 'w') as f:
    for row in get_code():
        f.write(row+'\r')
'''

for code in get_code():
    try:
        data=get_data(code)
        insert_sql(data,'stock_data')
    except:
        pass

'''
df=pd.read_sql('stock_data',engine)
print(len(df))
'''