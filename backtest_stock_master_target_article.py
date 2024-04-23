# 過濾出賺錢名單的標的文
import pandas as pd

from db_connect import conn
from import_tw_stock_analysis import MyStock

# 用股票高手的ID去找出他的[標的]文章
# 因為股票高手可能有多篇年報心得文，為避免資料重複，先以ID為group，把AID的資訊串在一起
stock_master = pd.read_sql("""
select 
    A.author0,A.author_annual_report,
    B.title target_title,B.date_format target_date,B.url target_url
from 
    (SELECT 
      author0,
      GROUP_CONCAT( title || ' - ' ||url || date_format || ' - ' || message, '; ') AS author_annual_report
    FROM 
      stock_god_info
    GROUP BY 
      author0) A
inner join
    info B
on
    A.author0 = B.author0 and
    B.category = '標的' and
    B.is_re <> 1
""", conn)

stock_master.sort_values(by=['target_date'], inplace=True, ignore_index=True, ascending=False)
# 塞選出target_title內四位數字的標的代碼
import re

# 抓出標的代碼
stock_master['target_code'] = stock_master['target_title'].apply(
    lambda x: re.search(r'\d{4}', x).group() if re.search(r'\d{4}', x) else None)

# 去掉部分標的代碼為空的資料
stock_master = stock_master[stock_master['target_code'].notnull()]

# 用回測的方式去找出標的的報酬率
profit_dict_list = []
for itr, row in stock_master.iterrows():
    # row = see.loc[12]
    tmp_profit_dict = {}
    tmp_profit_dict['target_code'] = row['target_code']
    tmp_profit_dict['target_date'] = row['target_date']
    try:
        tmp_profit_dict.update(MyStock(row['target_code']).back_test(pd.to_datetime(row['target_date'])))
        tmp_profit_dict['is_success_get_data'] = True
    except:
        tmp_profit_dict['is_success_get_data'] = False
    profit_dict_list.append(tmp_profit_dict)

profit_df = pd.DataFrame(profit_dict_list)
result = stock_master.merge(profit_df, on=['target_code', 'target_date'], how='left')

result.to_csv('target.csv', encoding='BIG5')
