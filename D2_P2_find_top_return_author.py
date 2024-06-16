# 分析
import pandas as pd

from config import csv_path, xlsx_path, return_days_map, return_days_int_list, return_days_adj_show_str_list

# 計算各author的summary
# all_target_article_return.csv 資料來自於 D2_P1_summary_target_article_return.py
all_target_article_return_df = pd.read_csv(csv_path + 'all_target_article_return.csv', encoding='UTF-8')
print(f"Total have {len(all_target_article_return_df)} articles need summary before filter.")

# 去掉部分標的代碼為空的資料
all_target_article_return_df = all_target_article_return_df[
    all_target_article_return_df['target_code'].notnull()].reset_index(drop=True)

# 留下有計算return的資料
all_target_article_return_df = all_target_article_return_df[
    all_target_article_return_df['is_success_get_return_data'] == True].reset_index(drop=True)

# 留下至少有最短日期的績效的標的文
min_days_str = return_days_map[str(min(return_days_int_list))]
all_target_article_return_df = all_target_article_return_df[
    all_target_article_return_df[min_days_str].notnull()].reset_index(drop=True)

# 判斷標題是否有含'空'字，否則都判定為做多，去掉做空的標的文
all_target_article_return_df = all_target_article_return_df[
    ~all_target_article_return_df['title'].str.contains('空')].reset_index(drop=True)

print(f"Total have {len(all_target_article_return_df)} articles need summary after filter.")

# 時間排序
all_target_article_return_df.sort_values(by=['post_date'], inplace=True, ignore_index=True, ascending=False)

# 用group by 作者算出平均獲利與發文筆數
all_target_article_return_df['article_CT'] = 1

agg_dict = {col: 'mean' for col in return_days_adj_show_str_list}
agg_dict['article_CT'] = 'count'

# 把等等要算mean的欄位轉成float
for tmp_col in return_days_adj_show_str_list:
    all_target_article_return_df[tmp_col] = all_target_article_return_df[tmp_col].astype(float)

author_return_eval_df = all_target_article_return_df.groupby(['author0'], as_index=False).aggregate(
    agg_dict).sort_values(by=['article_CT'], ascending=False, ignore_index=True)
print(f"Total had {len(author_return_eval_df)} authors be summarized.")

# xlsx
author_return_eval_df.to_excel(xlsx_path + 'author_return_summary.xlsx', index=False)
# author_return_eval_df = pd.read_excel(xlsx_path + 'author_return_summary.xlsx')
# csv
author_return_eval_df.to_csv(csv_path + 'author_return_summary.csv', encoding='UTF-8', index=False)
print(f"Author return summary csv file has been saved.")

# 開始找出高績效作者
# 最少有五筆資料的人
author_return_eval_df = author_return_eval_df[author_return_eval_df['article_CT'] >= 5].reset_index(drop=True)

top_return_author_list = []
# 個時間段獲利最高的top n
for tmp_day in return_days_adj_show_str_list:
    tmp_top_return_author = author_return_eval_df.sort_values(by=[tmp_day], ascending=False).head(20)
    top_return_author_list.append(tmp_top_return_author)

# 高手ID全部取出
top_return_author = pd.concat(top_return_author_list).drop_duplicates().sort_values(by=['article_CT'],
                                                                                    ascending=False).reset_index(
    drop=True)
print(f"Total had {len(top_return_author)} top return authors.")

# 寫成csv
top_return_author.to_csv(csv_path + 'top_return_author.csv', encoding='UTF-8', index=False)
print(f"Top return authors csv file has been saved.")
# 查看某人
# see = all_target_article_return_df[all_target_article_return_df['author0'] == 'agogo1202 ']
