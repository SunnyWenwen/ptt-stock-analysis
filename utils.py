import re

import pandas as pd

from config import return_days_int_list
from import_tw_stock_analysis import MyStock


def get_stock_code_from_title(stock_code):
    return re.search(r'\d{4}', stock_code).group() if re.search(r'\d{4}', stock_code) else None


def get_stock_code_and_date_return(stock_code_and_date_df, code_col_name, date_col_name):
    # 共有幾筆資料
    print(f'Total have {len(stock_code_and_date_df)} articles need to cal return.')

    # 以發文時間為基準找出標的的報酬率
    return_dict_list = []
    for itr, row in stock_code_and_date_df.iterrows():
        # 每100筆資料印一次
        dash = '-' * 10
        if itr % 50 == 0:
            print(f'{dash}Already processed {itr}/ {len(stock_code_and_date_df)} articles.{dash}')
        # row = see.loc[12]
        tmp_return_dict = {}
        tmp_return_dict[code_col_name] = row[code_col_name]
        tmp_return_dict[date_col_name] = row[date_col_name]
        try:
            tmp_return_dict.update(
                MyStock(row[code_col_name], initial_fetch=False, silent=True).cal_return(
                    pd.to_datetime(row[date_col_name]),
                    test_day_list=return_days_int_list,
                    silent=True))
            tmp_return_dict['is_success_get_return_data'] = True
        except:
            tmp_return_dict['is_success_get_return_data'] = False
        return_dict_list.append(tmp_return_dict)
    return pd.DataFrame(return_dict_list)
