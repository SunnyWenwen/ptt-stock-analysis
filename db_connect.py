import sqlite3

import pandas as pd

from config import db_path

ptt_date_db_file = db_path + "ptt_article_data.db"
conn = sqlite3.connect(ptt_date_db_file)

# rows = conn.execute("select * from ppt_article_ids")
if __name__ == 'main':
    title_df = pd.read_sql("select * from ppt_article_ids", conn)
