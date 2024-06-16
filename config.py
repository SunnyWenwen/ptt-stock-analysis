import datetime

mode = 'dev'  # 'dev' or 'prod'

date_format = '%Y-%m-%d %H:%M:%S'
upload_date = datetime.datetime.now().strftime(date_format)
pre_30_day = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime(date_format)

today_ymd = datetime.datetime.now().strftime('%Y%m%d')

# cal return days
return_days_map = {'10': '10days_return', '30': '30days_return', '60': '60days_return', '120': '120days_return',
                   '180': '180days_return', '360': '360days_return'}
return_days_int_list = [int(x) for x in return_days_map.keys()]
return_days_str_list = [str(x) for x in return_days_map.keys()]
return_days_show_str_list = [str(x) for x in return_days_map.values()]

return_days_adj_map = return_days_map.copy()
return_days_adj_map.update({key + '_adj': value + '_adj' for key, value in return_days_map.items()})
return_days_adj_str_list = [str(x) for x in return_days_adj_map.keys()]
return_days_adj_show_str_list = [str(x) for x in return_days_adj_map.values()]

return_days_only_adj_show_str_list = [str(x) for x in return_days_adj_map.values() if x not in return_days_map.values()]

# recent_fluctuation
recent_fluctuation_days_list = [5, 10, 30, 60]
recent_fluctuation_days_str_list = [str(x) for x in recent_fluctuation_days_list]

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
