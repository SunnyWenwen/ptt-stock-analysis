# 找出高表現作者最近的發文
import pandas as pd

from config import csv_path, xlsx_path, pre_30_day
from db_connect import conn

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

print(f'lately article count: {len(top_return_author_latest_article)}')
# 把他們績效串到後面
top_return_author_latest_article = top_return_author_latest_article.merge(top_return_author_df, on='author0',
                                                                          how='left')

# 時間近的在前面
top_return_author_latest_article.sort_values(by=['date_format'], inplace=True, ignore_index=True, ascending=False)

# 寫成xlsx and csv
top_return_author_latest_article.to_excel(xlsx_path + 'top_return_author_latest_article.xlsx', index=False)
top_return_author_latest_article.to_csv(csv_path + 'top_return_author_latest_article.csv', encoding='UTF-8',
                                        index=False)

print('Complete get top_return_author_latest_article')
