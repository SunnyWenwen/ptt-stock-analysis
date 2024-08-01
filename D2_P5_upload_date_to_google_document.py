import pandas as pd
import pygsheets

from config import xlsx_path

gc = pygsheets.authorize(service_file='upload_date_to_google_document.json')
sht = gc.open_by_url(
    'https://docs.google.com/spreadsheets/d/1vNf7zi-a1KM_c2CsLQL_HmO6rdsfwmPHnjVC-pAEP6k/edit#gid=0'
)
# 列出所有
wks_list = sht.worksheets()
print(wks_list)

# 讀取需要寫入的檔案
all_target_article_return_df = pd.read_excel(xlsx_path + 'all_target_article_return.xlsx')
all_target_article_return_df.rename(
    columns={'title': 'post_title', 'url': 'post_url', 'target_code': 'recommendation_code'}, inplace=True)
author_return_summary = pd.read_excel(xlsx_path + 'author_return_summary.xlsx')

# 開始寫入

print('Start upload to google document.')
print('Start upload all_recommendation_post_return.')
# 先寫入all_target_article_return
wks = sht.worksheet_by_title('all_recommendation_post_return')
# 可能會超過上限，最新的排前面
all_target_article_return_df.sort_values(by=['post_date'], inplace=True, ignore_index=True, ascending=False)

# error應該是googleapiclient.errors.HttpError
try:
    wks.set_dataframe(all_target_article_return_df, 'A1')  # 從欄位 A1 開始
except Exception as e:
    print(e)
print('all_recommendation_post_return has been uploaded.')

print('Start upload author_return_summary.')
# 寫入author_return_summary
wks = sht.worksheet_by_title('author_return_summary')
wks.set_dataframe(author_return_summary, 'A1')  # 從欄位 A1 開始
print('author_return_summary has been uploaded.')

print('Complete upload to google document.')

# # 測試df
# df = pd.DataFrame({
#     'Name': ['Alice', 'Bob', 'Charlie'],
#     'Age': [25, 30, 35],
#     'City': ['New York', 'Los Angeles', 'Chicago']
# })
# wks.set_dataframe(df, 'A1')  # 從欄位 A1 開始
