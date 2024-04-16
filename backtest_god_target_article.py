# 過濾出賺錢名單的標的文
import pandas as pd

from db_connect import conn
from import_tw_stock_analysis import MyStock

see = pd.read_sql("""
select 
    A.AID report_AID,A.author,A.url report_url,A.date_format report_date,A.message report_performance,
    B.title target_title,B.date_format target_date,B.url target_url,B.is_re target_is_re 
from 
    stock_god_info A 
inner join 
    info B
on 
    A.author0 = B.author0 and 
    b.category = '標的' and
    target_is_re <> 1""", conn)

see.sort_values(by=['target_date'], inplace=True, ignore_index=True, ascending=False)
# 塞選出target_title內四位數字的標的代碼
import re

# 抓出標的代碼
see['target_code'] = see['target_title'].apply(
    lambda x: re.search(r'\d{4}', x).group() if re.search(r'\d{4}', x) else None)

# 去掉部分標的代碼為空的資料
see = see[see['target_code'].notnull()]

profit_list = []

for itr, row in see.iterrows():
    # row = see.loc[12]
    try:
        tmp_profit = MyStock(row['target_code']).back_test(pd.to_datetime(row['target_date']))
        tmp_profit['target_code'] = row['target_code']
        tmp_profit['target_date'] = row['target_date']
        profit_list.append(tmp_profit)
    except:
        see.loc[itr, 'target_profit'] = None

profit_list = pd.DataFrame(profit_list)
see = see.merge(profit_list, on=['target_code', 'target_date'], how='left')

# TODO 同一個人有多篇心得文時，資料會重複，待修正

see.to_csv('target.csv', encoding='BIG5')
