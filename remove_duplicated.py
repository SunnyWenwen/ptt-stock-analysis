import pandas as pd
from db_connect import conn
import collections

cur = conn.cursor()


def check_duplicated(table, unique_col_name):  # table = 'stock_god_info';unique_col_name = 'AID'
    # check duplicated
    target_data = pd.read_sql(f"select {unique_col_name} from {table}", conn)
    ct = collections.Counter(target_data[unique_col_name])
    ct_ct = collections.Counter(ct.values())
    if len(ct_ct) <= 1:
        print(f'{table}data no duplicated')
        print(f"    {str(ct_ct)}")
        return False
    else:
        print(f'{table}data duplicated')
        print(f"    {str(ct_ct)}")
        return True


def remove_duplicated(table, unique_col_name):
    # remove duplicated
    if check_duplicated(table, unique_col_name):
        print(f'use {unique_col_name} remove {table} duplicated data')
        sql = f'''
        DELETE FROM {table}
        WHERE rowid NOT IN (
            SELECT MIN(rowid)
            FROM {table}
            GROUP BY {unique_col_name}
        )'''
        try:
            # 執行 SQL 語句
            cur.execute(sql)

            # 提交事務
            conn.commit()

            print("remove duplicated data successfully")
        except Exception as e:
            # 如果出錯了，打印錯誤信息
            print("error", e)
        print('result after remove duplicated data')
        check_duplicated(table, unique_col_name)
    else:
        print('dont need to remove duplicated data')


remove_duplicated('stock_god_info', 'AID')
remove_duplicated('ppt_article_ids', 'AID')
remove_duplicated('ppt_article_details', 'AID')
remove_duplicated('gpt_res', 'AID')
