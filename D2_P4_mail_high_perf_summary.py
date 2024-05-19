import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pandas as pd

from config import csv_path, mail_path, today_ymd, username, host, port, password

print('Start get high_perf_auth_latest_article')
# 讀取高績效作者最近的發文
high_perf_auth_latest_article = pd.read_csv(csv_path + 'high_perf_auth_latest_article.csv')
# 留下部分欄位就好
high_perf_auth_latest_article = high_perf_auth_latest_article[
    ['author0', 'title', 'date_format', 'url', '10', '30', '60', '120', '180', '360', 'article_CT']]

# 部分欄位取到小數點後兩位
high_perf_auth_latest_article[['10', '30', '60', '120', '180', '360']] = high_perf_auth_latest_article[
    ['10', '30', '60', '120', '180', '360']].round(2)

# 欄位重新命名
high_perf_auth_latest_article.rename(
    columns={'author0': '作者', 'title': '標題', 'date_format': '發文日期', 'url': 'ptt網址', '10': '歷史10天績效',
             '30': '歷史30天績效', '60': '歷史60天績效', '120': '歷史120天績效', '180': '歷史180天績效',
             '360': '歷史360天績效', 'article_CT': '發標的文次數'}, inplace=True)

# 讀取高績效作者之前的發文紀錄以及回測績效
target_article_profit_df = pd.read_csv(csv_path + 'target_article_profit.csv')

# 留下那些高績效作者就好
target_article_profit_df = target_article_profit_df[
    target_article_profit_df['author0'].isin(high_perf_auth_latest_article['作者'])]
target_article_profit_df.sort_values(by=['author0', 'date_format'], inplace=True, ignore_index=True, ascending=False)
# 輸出成xlsx到mail_path
target_article_profit_df.to_excel(mail_path + 'mail_attachment.xlsx', index=False)

print('Complete get high_perf_auth_latest_article')

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
subject = f'[{today_ymd}]PTT high-performing authors latest investment thesis.'
show_to_list = ['p147896325p@gmail.com', 'ww61220@gmail.com']
show_from = username

msg = MIMEMultipart()
msg['Subject'] = subject
msg['From'] = show_from
msg['To'] = ";".join(show_to_list)

# plain text 1
plain_text_part = MIMEText("Within 30 days, the investment thesis of high-performing authors.\n", 'plain')
msg.attach(plain_text_part)  # 郵件純文字內容

# dataframe
html_table = high_perf_auth_latest_article.to_html(index=False)
html_part = MIMEText(html_table, 'html')
msg.attach(html_part)

# plain text 2
plain_text_part = MIMEText("\nFor historical investment thesis by each author, please refer to the attachment.\n",
                           'plain')
msg.attach(plain_text_part)

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
