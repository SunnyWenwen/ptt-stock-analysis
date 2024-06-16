# 找出高表現作者最近的發文
import pandas as pd

from config import csv_path, xlsx_path, pre_30_day, recent_fluctuation_days_list, recent_fluctuation_days_str_list, \
    return_days_show_str_list, return_days_adj_show_str_list, return_days_only_adj_show_str_list
from db_connect import conn
from utils import get_stock_code_from_title, get_stock_code_recent_fluctuation

print('Start get top_return_author_latest_article')
# 讀取高表現作者
top_return_author_df = pd.read_csv(csv_path + 'top_return_author.csv', encoding='UTF-8')

# 看高表現作者最近有沒有發標的文
high_perf_auth_article = pd.read_sql(
    """SELECT * FROM ppt_article_details WHERE category = '標的' and author0 in ('{}')""".format(
        "','".join(top_return_author_df['author0'])), conn)

# 進30天的文章
top_return_author_latest_article = high_perf_auth_article[
    high_perf_auth_article['date_format'] >= pre_30_day].reset_index(drop=True)

# 留下指定欄位
top_return_author_latest_article = top_return_author_latest_article[['author0', 'title', 'date_format', 'url']]
print(f'lately article count: {len(top_return_author_latest_article)}')

# 抓出標的代碼
top_return_author_latest_article['target_code'] = top_return_author_latest_article['title'].apply(
    get_stock_code_from_title)

# 計算近期漲幅
stock_recent_fluctuation_df = get_stock_code_recent_fluctuation(set(top_return_author_latest_article['target_code']),
                                                                recent_fluctuation_days_list)

# 把近期漲幅欄位串起來
stock_recent_fluctuation_df['recent_fluctuation'] = stock_recent_fluctuation_df.apply(
    lambda x: '/'.join(map(str, list(x[recent_fluctuation_days_str_list]))), axis=1)
# 去掉recent_fluctuation_days_str_list欄位
stock_recent_fluctuation_df.drop(columns=recent_fluctuation_days_str_list, inplace=True)

# 去掉is_success_get_recent_fluctuation欄位
stock_recent_fluctuation_df.drop(columns=['is_success_get_recent_fluctuation'], inplace=True)

# 把漲幅串到後面
top_return_author_latest_article = top_return_author_latest_article.merge(stock_recent_fluctuation_df, on='target_code',
                                                                          how='left')

# 把他們績效串到後面
# 先留下小數點後兩位
top_return_author_df[return_days_adj_show_str_list] = top_return_author_df[
    return_days_adj_show_str_list].round(1)

# 先把績效欄位合併(原始與adj分開合併)
# 原始合併
top_return_author_df['all_days_return'] = top_return_author_df.apply(
    lambda x: '/'.join(map(str, x[return_days_show_str_list])), axis=1)

# adj合併
top_return_author_df['all_days_return_adj'] = top_return_author_df.apply(
    lambda x: '/'.join(map(str, x[return_days_only_adj_show_str_list])), axis=1)

# 去掉return_days_show_str_list
top_return_author_df.drop(columns=return_days_adj_show_str_list, inplace=True)

top_return_author_latest_article = top_return_author_latest_article.merge(top_return_author_df, on='author0',
                                                                          how='left')

# 時間近的在前面
top_return_author_latest_article.sort_values(by=['date_format'], inplace=True, ignore_index=True, ascending=False)

# 寫成xlsx and csv
top_return_author_latest_article.to_excel(xlsx_path + 'top_return_author_latest_article.xlsx', index=False)
top_return_author_latest_article.to_csv(csv_path + 'top_return_author_latest_article.csv', encoding='UTF-8',
                                        index=False)

print('Complete get top_return_author_latest_article')
