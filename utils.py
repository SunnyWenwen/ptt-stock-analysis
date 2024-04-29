import re

import pandas as pd

from import_tw_stock_analysis import MyStock


def get_stock_code_from_title(stock_code):
    return re.search(r'\d{4}', stock_code).group() if re.search(r'\d{4}', stock_code) else None


def back_test_stock_code_and_date(stock_code_and_date_df, code_col_name, date_col_name):
    # 共有幾筆資料
    print('共有', len(stock_code_and_date_df), '筆資料需要回測')

    # 用回測的方式去找出標的的報酬率
    profit_dict_list = []
    for itr, row in stock_code_and_date_df.iterrows():
        # row = see.loc[12]
        tmp_profit_dict = {}
        tmp_profit_dict[code_col_name] = row[code_col_name]
        tmp_profit_dict[date_col_name] = row[date_col_name]
        try:
            tmp_profit_dict.update(
                MyStock(row[code_col_name], initial_fetch=False).back_test(pd.to_datetime(row[date_col_name])))
            tmp_profit_dict['is_success_get_data'] = True
        except:
            tmp_profit_dict['is_success_get_data'] = False
        profit_dict_list.append(tmp_profit_dict)
    return pd.DataFrame(profit_dict_list)
