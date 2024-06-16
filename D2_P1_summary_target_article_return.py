# 計算所有標的文的報酬率
# 抓取ptt所有標的文
import pandas as pd

from config import csv_path, xlsx_path, upload_date, return_days_str_list, return_days_map, return_days_adj_str_list, \
    return_days_adj_map, mode
from db_connect import conn
from utils import get_stock_code_from_title, get_stock_code_and_date_return

ppt_article_ori_df = pd.read_sql("SELECT * FROM ppt_article_details WHERE category = '標的'", conn)

ppt_article = ppt_article_ori_df.copy()
# 抓出標的代碼
ppt_article['target_code'] = ppt_article['title'].apply(get_stock_code_from_title)

# 去掉回文的標的文
ppt_article = ppt_article[ppt_article['is_re'] != 1].reset_index(drop=True)

# 測試時用前500筆計算就好
if mode == 'dev':
    ppt_article = ppt_article.head(500)

# 去掉部分標的代碼為空的資料
ppt_article_for_return = ppt_article[ppt_article['target_code'].notnull()].reset_index(drop=True)

# 用發文的時間為基準，計算標的的報酬率
target_code_return_df = get_stock_code_and_date_return(ppt_article_for_return[['target_code', 'date_format']],
                                                       'target_code', 'date_format', adjust_by_taiex=True)
# 把column名稱改成str
target_code_return_df.columns = target_code_return_df.columns.astype(str)

# 暫時儲存起來
target_code_return_df.to_csv(csv_path + 'target_code_return.csv', encoding='UTF-8', index=False)
# target_code_return_df = pd.read_csv(csv_path+'target_code_return.csv', encoding='UTF-8')
# target_code_return_df['target_code'] = target_code_return_df['target_code'].astype(str)

# 合併回原本的資料
all_target_article_return_df = ppt_article.merge(target_code_return_df, on=['target_code', 'date_format'], how='left')

# 留下指定的欄位
all_target_article_return_df = all_target_article_return_df[
    ['author0', 'title', 'date_format', 'url', 'target_code',
     'is_success_get_return_data'] + return_days_adj_str_list].reset_index(drop=True)

# 重新命名欄位
all_target_article_return_df.rename(columns=return_days_adj_map, inplace=True)
all_target_article_return_df.rename(columns={'date_format': 'post_date'}, inplace=True)

all_target_article_return_df['cal_return_date'] = upload_date
all_target_article_return_df.sort_values(by=['post_date'], inplace=True, ignore_index=True, ascending=True)

# 儲存
all_target_article_return_df.to_csv(csv_path + 'all_target_article_return.csv', encoding='UTF-8', index=False)
# all_target_article_return_df = pd.read_csv(csv_path+'all_target_article_return.csv', encoding='UTF-8')

# xlsm格式
all_target_article_return_df.to_excel(xlsx_path + 'all_target_article_return.xlsx', index=False)
