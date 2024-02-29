# 過濾出賺錢名單的標的文
import pandas as pd

from db_connect import conn

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

see['target_code'] = see['target_title'].apply(
    lambda x: re.search(r'\d{4}', x).group() if re.search(r'\d{4}', x) else None)

# 用標的代碼去回測標的收益
from get_stock_price_data import back_test

see['target_profit'] = see.apply(
    lambda x: back_test(x['target_code'], pd.to_datetime(x['target_date']), silent=True)[-1], axis=1)
# TODO 3332的資料有問題，代確認原因

see.to_csv('target.csv', encoding='BIG5')
