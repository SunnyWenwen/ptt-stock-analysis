# 從另一個資料夾import module
import sys
import os

# 添加上層目錄到sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../tw-stock-analysis')))
from get_stock_price_data import MyStock

MyStock = MyStock

from create_downloaded_stock_price_db import create_stock_price_table, create_stock_header_table

create_stock_price_table()
create_stock_header_table()
