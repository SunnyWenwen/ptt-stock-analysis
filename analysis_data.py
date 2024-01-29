from const import upload_date
from db_connect import conn
import pandas as pd
import re

from gpt_api import stock_experience_report_summarizer

# 塞選出心得文&年報
target_data = pd.read_sql("select * from info", conn)
target_data = target_data[target_data['category'] == '心得']
target_data = target_data[target_data['title'].apply(lambda x: bool(re.search(r'2023|年報', x)))]
target_data = target_data[target_data['is_re'] == 0]
target_data = target_data.reset_index(drop=True)

# start use gpt analysis article
res_list = []
for i, tmp_target_data in target_data.iterrows():
    tmp_res = stock_experience_report_summarizer(tmp_target_data)
    res_list.append(tmp_res)

all_res_df = pd.DataFrame(res_list)
all_res_df['upload_date'] = upload_date

all_res_df.to_sql('gpt_res', conn, if_exists='append', index=False)
# all_res_df.to_sql('gpt_res', conn, if_exists='replace', index=False)
conn.commit()

# all_res_df.to_csv('all_res_df.csv',encoding='BIG5')


# 過濾出賺錢名單塞入table
gpt_res = pd.read_sql("select * from gpt_res", conn)
gpt_res = gpt_res[gpt_res['is_report'] == 'Y']
gpt_res = gpt_res[gpt_res['is_profitable'] == 'Y']
gpt_res = gpt_res[gpt_res['annual_rate'].apply(bool)]
gpt_res = gpt_res.reset_index(drop=True)

gpt_res.to_sql('stock_god_info', conn, if_exists='replace', index=False)
