import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pandas as pd

from config import csv_path, mail_path, today_ymd, username, host, port, password, return_days_show_str_list, \
    recent_fluctuation_days_str_list, return_days_str_list, mode

print('Start get top_return_author_latest_article')
# 讀取高績效作者最近的發文
high_perf_auth_latest_article = pd.read_csv(csv_path + 'top_return_author_latest_article.csv')

# 欄位重新命名
rename_dict = {'author0': '作者', 'title': '標題', 'date_format': '發文日期', 'url': 'ptt網址',
               'target_code': '標的代碼',
               'recent_fluctuation': f"該標的近期漲幅(%)[{'/'.join(recent_fluctuation_days_str_list)} days]",
               'article_CT': '[作者歷史]標的文章數',
               'all_days_return': f"[作者歷史]標的平均績效(%)[{'/'.join(return_days_str_list)} days]",
               'all_days_return_adj': f"[作者歷史]標的平均績效 用大盤校正(%)[{'/'.join(return_days_str_list)} days]"}
high_perf_auth_latest_article.rename(columns=rename_dict, inplace=True)

# 讀取高績效作者之前的發文紀錄以及績效
all_target_article_return_df = pd.read_csv(csv_path + 'all_target_article_return.csv')

# 留下那些高績效作者就好
all_target_article_return_df = all_target_article_return_df[
    all_target_article_return_df['author0'].isin(high_perf_auth_latest_article['作者'])]
all_target_article_return_df.sort_values(by=['author0', 'post_date'], inplace=True, ignore_index=True,
                                         ascending=False)
# 輸出成xlsx到mail_path
all_target_article_return_df.to_excel(mail_path + 'mail_attachment.xlsx', index=False)

print('Complete get top_return_author_latest_article')
print('Start send mail')
# start send mail
real_from_email = username

# # sample file
# df = pd.DataFrame({
#     'Name': ['Alice', 'Bob', 'Charlie'],
#     'Age': [25, 30, 35],
#     'City': ['New York', 'Los Angeles', 'Chicago']
# })
# csv_file = 'dataframe.csv'
# df.to_csv(csv_file, index=False)

# context
subject = f'[{today_ymd}]PTT high-performing authors latest recommendation post.'
mail_list = pd.read_csv('mail_list.csv')
if mode == 'dev':
    show_to_list = ['u102021112@gmail.com', 'p147896325p@gmail.com']
else:
    show_to_list = mail_list['mail_list'].tolist()
show_from = username

msg = MIMEMultipart()
msg['Subject'] = subject
msg['From'] = show_from
msg['To'] = username
# msg['Bcc'] = ";".join(show_to_list)

# plain text 1
plain_text_part = MIMEText("Within 30 days, the recommendation post of high-performing authors.\n", 'plain')
msg.attach(plain_text_part)  # 郵件純文字內容

# dataframe
html_table = high_perf_auth_latest_article.to_html(index=False)
html_part = MIMEText(html_table, 'html')
msg.attach(html_part)

# plain text 2
plain_text_part = MIMEText("\nFor historical recommendation post by each author, please refer to the attachment.\n",
                           'plain')
msg.attach(plain_text_part)

# plain text 3
plain_text_part = MIMEText(
    """
    <html>
        <body>
            <p> 
        More information about recommendation post return and author return summary can 
        click <a href="https://docs.google.com/spreadsheets/d/1vNf7zi-a1KM_c2CsLQL_HmO6rdsfwmPHnjVC-pAEP6k/edit?usp=sharing">link</a> 
            </p>
        </body>
    </html>""",
    'html')

msg.attach(plain_text_part)
# gid=2018541462
# attach file
# 添加 CSV 文件作为附件
with open(mail_path + 'mail_attachment.xlsx', 'rb') as file:
    part = MIMEApplication(file.read(), Name='mail_attachment.xlsx')
    part['Content-Disposition'] = 'attachment; filename="mail_attachment.xlsx"'
    msg.attach(part)

try:
    # sent mail
    server = smtplib.SMTP_SSL(host, port)
    # server.starttls()
    server.login(username, password)
    real_to_list = show_to_list

    res = server.sendmail(real_from_email, real_to_list, msg.as_string())
    if res:
        print('Some emails failed to send:')
        for recipient, (code, error) in res.items():
            print(f'{recipient}: {code} {error}')
    else:
        print('All emails sent successfully')
    server.close()
except Exception as e:
    print(f'Error sending mail: {e}')
