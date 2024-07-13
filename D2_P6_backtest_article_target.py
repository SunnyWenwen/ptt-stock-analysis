import pandas as pd
from numpy import mean

from config import return_days_adj_str_list
from db_connect import conn
from utils import get_stock_code_and_date_return

# 抓出之前推薦過的標的文
top_return_author_latest_article_log_df = pd.read_sql("select * from top_return_author_latest_article_log", conn)

# 挑出有標的文的資料
top_return_author_latest_article_log_df = top_return_author_latest_article_log_df[
    top_return_author_latest_article_log_df['target_code'].notnull()]

# 用first recommend date去回測
back_test_result_ori = get_stock_code_and_date_return(
    top_return_author_latest_article_log_df[['target_code', 'first_recommend_date']], 'target_code',
    'first_recommend_date', adjust_by_taiex=True)

# 計算平均績效，全部欄位取平均
back_test_result = back_test_result_ori[return_days_adj_str_list].mean(axis=0)
