# 分析
import pandas as pd

from config import csv_path, xlsx_path

# all_target_article_return.csv 資料來自於 D2_P1_summary_target_article_return.py
all_target_article_return_df = pd.read_csv(csv_path + 'all_target_article_return.csv', encoding='UTF-8')
# 留下有抓到資料的
all_target_article_return_df = all_target_article_return_df[
    all_target_article_return_df['is_success_get_data'] == True].reset_index(drop=True)
# 留下至少10天的績效的標的文
all_target_article_return_df = all_target_article_return_df[all_target_article_return_df['10'].notnull()].reset_index(
    drop=True)

# 判斷標題是否有含'空'字，否則都判定為做多，去掉做空的標的文
all_target_article_return_df = all_target_article_return_df[
    ~all_target_article_return_df['title'].str.contains('空')].reset_index(drop=True)

# 時間排序
all_target_article_return_df.sort_values(by=['date_format'], inplace=True, ignore_index=True, ascending=False)
# 算出獲利最大值
all_target_article_return_df['max'] = all_target_article_return_df.apply(
    lambda x: x[['10', '30', '60', '120', '180', '360']].dropna().max(), axis=1)
all_target_article_return_df['article_CT'] = 1

# 用group by 作者算出平均獲利
author_return_eval_df = all_target_article_return_df.groupby(['author0'], as_index=False).aggregate(
    {'10': 'mean', '30': 'mean', '60': 'mean', '120': 'mean', '180': 'mean', '360': 'mean', 'max': 'mean',
     'article_CT': 'count'}).sort_values(by=['max'], ascending=False)

# xlsx
author_return_eval_df.to_excel(xlsx_path + 'author_return_summary.xlsx', index=False)
# author_return_eval_df = pd.read_excel(xlsx_path + 'author_return_eval_df.xlsx')
# csv
author_return_eval_df.to_csv(csv_path + 'author_return_summary.csv', encoding='UTF-8', index=False)

# 開始找出高績效作者
# 最少有五筆資料的人
author_return_eval_df = author_return_eval_df[author_return_eval_df['article_CT'] >= 5].reset_index(drop=True)

# 10天獲利最高的人
top_10day_return = author_return_eval_df.sort_values(by=['10'], ascending=False).head(10)
# 30天獲利最高的人
top_30day_return = author_return_eval_df.sort_values(by=['30'], ascending=False).head(10)
# 60天獲利最高的人
top_60day_return = author_return_eval_df.sort_values(by=['60'], ascending=False).head(10)
# 120天獲利最高的人
top_120day_return = author_return_eval_df.sort_values(by=['120'], ascending=False).head(10)
# 180天獲利最高的人
top_180day_return = author_return_eval_df.sort_values(by=['180'], ascending=False).head(10)
# 360天獲利最高的人
top_360day_return = author_return_eval_df.sort_values(by=['360'], ascending=False).head(10)

# 高手ID全部取出
top_return_author = pd.concat(
    [top_10day_return['author0'], top_30day_return['author0'], top_60day_return['author0'],
     top_120day_return['author0'], top_180day_return['author0'],
     top_360day_return['author0']]).drop_duplicates().reset_index(drop=True)

top_return_author = author_return_eval_df[author_return_eval_df['author0'].isin(top_return_author)].reset_index(
    drop=True)
# 寫成csv
top_return_author.to_csv(csv_path + 'top_return_author.csv', encoding='UTF-8', index=False)

# 查看某人
# see = all_target_article_return_df[all_target_article_return_df['author0'] == 'agogo1202 ']
