# 計算所有標的文的報酬率
# 抓取ptt所有標的文
import pandas as pd

from config import csv_path, xlsx_path
from db_connect import conn
from utils import get_stock_code_from_title, get_stock_code_and_date_return

ppt_article_ori_df = pd.read_sql("SELECT * FROM ppt_article_details WHERE category = '標的'", conn)

ppt_article = ppt_article_ori_df.copy()
# 抓出標的代碼
ppt_article['target_code'] = ppt_article['title'].apply(get_stock_code_from_title)

# 去掉部分標的代碼為空的資料
ppt_article = ppt_article[ppt_article['target_code'].notnull()].reset_index(drop=True)

# 去掉回文的標的文
ppt_article = ppt_article[ppt_article['is_re'] != 1].reset_index(drop=True)

# 用發文的時間為基準，計算標的的報酬率
all_target_article_return_df = get_stock_code_and_date_return(ppt_article[['target_code', 'date_format']],
                                                              'target_code', 'date_format')

# 暫時儲存起來
all_target_article_return_df.to_csv(csv_path + 'all_target_article_return_df.csv', encoding='UTF-8', index=False)
# all_target_article_return_df = pd.read_csv('all_target_article_return_df.csv', encoding='UTF-8')

# 合併回原本的資料
result = ppt_article.merge(all_target_article_return_df, on=['target_code', 'date_format'], how='left')

# 儲存
result.to_csv(csv_path + 'all_target_article_return.csv', encoding='UTF-8', index=False)
# all_target_article_return_df = pd.read_csv(csv_path+'all_target_article_return.csv', encoding='UTF-8')

# xlsm格式
result.to_excel(xlsx_path + 'all_target_article_return.xlsx', index=False)
