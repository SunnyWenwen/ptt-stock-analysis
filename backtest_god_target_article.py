# 過濾出賺錢名單的標的文
import pandas as pd

from db_connect import conn

see = pd.read_sql("""select A.AID report_AID,A.author,A.url report_url,A.message,
                B.title target_title,B.date_format target_date,B.url target_url,B.is_re target_is_re from 
                stock_god_info A 
                inner join 
                info B
                on A.author0 = B.author0 and b.category = '標的' """, conn)
see.to_csv('target.csv', encoding='BIG5')
