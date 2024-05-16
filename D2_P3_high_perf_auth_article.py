# 找出高表現作者最近的發文
import pandas as pd

from config import csv_path, xlsx_path, pre_30_day
from db_connect import conn

# 讀取高表現作者
high_perf_auth_df = pd.read_csv(csv_path + 'high_perf_auth.csv', encoding='UTF-8')

# 看高表現作者最近有沒有發標的文
high_perf_auth_article = pd.read_sql("""SELECT * FROM info WHERE category = '標的' and author0 in ('{}')""".format(
    "','".join(high_perf_auth_df['author0'])), conn)

# 進30天的文章
high_perf_auth_latest_article = high_perf_auth_article[
    high_perf_auth_article['date_format'] >= pre_30_day].reset_index(drop=True)

# 把他們績效串到後面
high_perf_auth_latest_article = high_perf_auth_latest_article.merge(high_perf_auth_df, on='author0', how='left')

# 時間近的在前面
high_perf_auth_latest_article.sort_values(by=['date_format'], inplace=True, ignore_index=True, ascending=False)

# 寫成xlsx and csv
high_perf_auth_latest_article.to_excel(xlsx_path + 'high_perf_auth_latest_article.xlsx', index=False)
high_perf_auth_latest_article.to_csv(csv_path + 'high_perf_auth_latest_article.csv', encoding='UTF-8', index=False)
