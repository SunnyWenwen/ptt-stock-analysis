# 從另一個資料夾import module
import sys
import os
import datetime

# 添加上層目錄到sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../tw-stock-analysis')))
from get_stock_price_data import MyStock

MyStock = MyStock

from create_downloaded_stock_price_db import create_stock_price_table, create_stock_header_table

create_stock_price_table()
create_stock_header_table()

if __name__ == '__main__':
    print('測試是否可以使用tw-stock-analysis模組')
    stock = MyStock('2330')
    # 用一年前日期回測
    now = datetime.datetime.now()
    now -= datetime.timedelta(days=365)
    stock.back_test(now)
    print('測試完成')
