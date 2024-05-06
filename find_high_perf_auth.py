# 分析
import pandas as pd

from config import csv_path, xlsx_path

# target_article_profit.csv 資料來自於 backtest_all_target_article.py
result = pd.read_csv(csv_path + 'target_article_profit.csv', encoding='UTF-8')
# 留下有抓到資料的
result = result[result['is_success_get_data'] == True].reset_index(drop=True)
# 留下至少30天的
result = result[result['30'].notnull()].reset_index(drop=True)

# 時間排序
result.sort_values(by=['date_format'], inplace=True, ignore_index=True, ascending=False)
# 算出獲利最大值
result['max'] = result.apply(lambda x: x[['30', '60', '120', '180', '360']].dropna().max(), axis=1)
result['CT'] = 1

# 用group by 作者算出平均獲利
auth_perf_df = result.groupby(['author0'], as_index=False).aggregate(
    {'30': 'mean', '60': 'mean', '120': 'mean', '180': 'mean', '360': 'mean', 'max': 'mean', 'CT': 'count'
     }).sort_values(by=['max'], ascending=False)

# 最少有五筆資料的人
auth_perf_df = auth_perf_df[auth_perf_df['CT'] >= 5].reset_index(drop=True)

# xlsx
auth_perf_df.to_excel(xlsx_path + 'auth_perf_df.xlsx', index=False)

# 30天獲利最高的人
perf30 = auth_perf_df.sort_values(by=['30'], ascending=False).head(10)
# 60天獲利最高的人
perf60 = auth_perf_df.sort_values(by=['60'], ascending=False).head(10)
# 120天獲利最高的人
perf120 = auth_perf_df.sort_values(by=['120'], ascending=False).head(10)
# 180天獲利最高的人
perf180 = auth_perf_df.sort_values(by=['180'], ascending=False).head(10)
# 360天獲利最高的人
perf360 = auth_perf_df.sort_values(by=['360'], ascending=False).head(10)

# 高手ID全部取出
high_perf_auth = pd.concat([perf30['author0'], perf60['author0'], perf120['author0'], perf180['author0'],
                            perf360['author0']]).drop_duplicates().reset_index(drop=True)
# 寫成csv
high_perf_auth.to_csv(csv_path + 'high_perf_auth.csv', encoding='UTF-8', index=False)

# 查看某人
# see = result[result['author0'] == 'agogo1202 ']
