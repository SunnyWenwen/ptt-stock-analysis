import sqlite3

import pandas as pd

dbfile = "data.db"
conn = sqlite3.connect(dbfile)

# rows = conn.execute("select * from header")
if __name__ == 'main':
    title_df.to_sql('header', conn, if_exists='append', index=False)
    pd.read_sql("select * from header", conn)

    # sql_str = "insert into header(page,AID, title) values('{}','{}','{}');".format(newest_page, title_df['AID'][0], title_df['title'][0])
    conn.execute(sql_str)
    conn.commit()
