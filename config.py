import datetime

date_format = '%Y-%m-%d %H:%M:%S'
upload_date = datetime.datetime.now().strftime(date_format)
pre_30_day = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime(date_format)

today_ymd = datetime.datetime.now().strftime('%Y%m%d')

# db path
db_path = 'C:/Users/User/iCloudDrive/share_data/db/'
csv_path = 'C:/Users/User/iCloudDrive/share_data/csv/'
xlsx_path = 'C:/Users/User/iCloudDrive/share_data/xlsx/'
mail_path = 'C:/Users/User/iCloudDrive/share_data/mail/'

# mail info
host = "smtp.gmail.com"
port = 465
username = 'your_mail@gmail.com'
password = 'your_password'  # 若用gmail需要去gmail申請應用程式密碼

# 確認路徑是否存在
import os

if os.path.exists(db_path) is False:
    print(f'db_path not exist，please create folder {db_path}')
if os.path.exists(csv_path) is False:
    print(f'csv_path not exist，please create folder {csv_path}')
if os.path.exists(xlsx_path) is False:
    print(f'xlsx_path not exist，please create folder {xlsx_path}')
if os.path.exists(mail_path) is False:
    print(f'mail_path not exist，please create folder {mail_path}')
