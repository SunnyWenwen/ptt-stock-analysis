import datetime

date_format = '%Y-%m-%d %H:%M:%S'
upload_date = datetime.datetime.now().strftime(date_format)
# db path
db_path = 'C:/Users/User/iCloudDrive/share_data/db/'
csv_path = 'C:/Users/User/iCloudDrive/share_data/csv/'
xlsx_path = 'C:/Users/User/iCloudDrive/share_data/xlsx/'

# 確認路徑是否存在
import os

if os.path.exists(db_path) is False:
    print(f'db_path不存在，請建立資料夾{db_path}')
if os.path.exists(csv_path) is False:
    print(f'csv_path不存在，請建立資料夾{csv_path}')
if os.path.exists(xlsx_path) is False:
    print(f'xlsx_path不存在，請建立資料夾{xlsx_path}')
