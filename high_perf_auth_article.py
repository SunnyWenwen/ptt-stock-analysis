# 找出高表現作者最近的發文
import pandas as pd

from db_connect import conn

# 讀取高表現作者
high_perf_auth_df = pd.read_csv('high_perf_auth.csv', encoding='UTF-8')

# 看高表現作者最近有沒有發標的文
high_perf_auth_article = pd.read_sql("""SELECT * FROM info WHERE category = '標的' and author0 in ('{}')""".format(
    "','".join(high_perf_auth_df['author0'])), conn)
# 進30天的文章
high_perf_auth_latest_article = high_perf_auth_article[high_perf_auth_article['date_format'] >= '2024-04-01'].reset_index(drop=True)

# 寫成xlsx
high_perf_auth_latest_article.to_excel('high_perf_auth_latest_article.xlsx', index=False)
