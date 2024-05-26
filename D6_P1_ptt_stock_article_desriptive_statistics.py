import pandas as pd
from db_connect import conn
import matplotlib.pyplot as plt

# 讀取table所有以下載文章
df = pd.read_sql("select * from ppt_article_details", conn)

# 文章數
print(f"Total article count: {len(df)}")

# 最早的文章
start_date = df['date_format'].min()
start_date = start_date[:10]
print(f"Old post date: {start_date}")

# 最新的文章
end_date = df['date_format'].max()
end_date = end_date[:10]
print(f"New post date: {end_date}")

# 各文章種類的分布，多於10筆的才顯示
print(df['category'].value_counts()[df['category'].value_counts() > 10])

# 發文各月份的分布
print(df['date_format'].apply(lambda x: x[:7]).value_counts().sort_index())
# 畫成折線圖圖表
ym_df = df['date_format'].apply(lambda x: x[:7]).value_counts().sort_index()
ym_df.plot(kind='bar')

plt.xlabel('Year-Month')
plt.ylabel('Count')
plt.title(f'Count of Entries by Year-Month from {start_date} to {end_date}')
plt.xticks(rotation=45)
plt.show()

# 發文各年份的分布
print(df['date_format'].apply(lambda x: x[:4]).value_counts().sort_index())
# 畫成折線圖圖表
year_df = df['date_format'].apply(lambda x: x[:4]).value_counts().sort_index()
year_df.plot(kind='bar')

plt.xlabel('Year')
plt.ylabel('Count')
plt.title(f'Count of Entries by Year from {start_date} to {end_date}')
plt.show()

# 發文各周幾的分布
week_df = df['date_format'].apply(lambda x: pd.to_datetime(x).weekday()).value_counts().sort_index()
week_map = {0: 'Mon', 1: 'Tue', 2: 'Wed', 3: 'Thu', 4: 'Fri', 5: 'Sat', 6: 'Sun'}
week_df.index = week_df.index.map(week_map)
print(week_df)
# 畫成長條圖圖表
week_df.plot(kind='bar')

plt.xlabel('Weekday')
plt.ylabel('Count')
plt.title(f'Count of Entries by Weekday from {start_date} to {end_date}')
plt.show()
