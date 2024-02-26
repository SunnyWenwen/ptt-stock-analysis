from datetime import datetime, timedelta
from typing import List

from twstock import Stock


class MyStock(Stock):
    def __int__(self, sid: str, initial_fetch: bool = True):
        super().__init__(sid, initial_fetch)

    def fetch_from_to(self, from_year: int, from_month: int, to_year: int, to_month: int):
        """
        抓取指定時間區間的股價資料
        """
        self.raw_data = []
        self.data = []
        for year, month in self._month_year_iter(from_month, from_year, to_month, to_year):
            self.raw_data.append(self.fetcher.fetch(year, month, self.sid))
            self.data.extend(self.raw_data[-1]['data'])
        return self.data

    def get_target_date_n_daily_average_price(self, target_date: datetime, n_daily_average: int):
        """
        取得指定日期的N日均價。若當日無資料，則往前找到有資料的日期。
        stock = MyStock('2330')
        stock.get_target_date_n_daily_average_price(datetime(year=2024, month=2, day=25), 60)
        """

        # 確保可以抓到N日均價
        pre_month = target_date - timedelta(days=n_daily_average * 2)
        self.fetch_from_to(pre_month.year, pre_month.month, target_date.year, target_date.month)
        # 去掉晚於target_date的資料
        self.data = [tmp_data for tmp_data in self.data if tmp_data.date <= target_date]
        # 取得target_date的N日均價

        return self.moving_average(self.price, n_daily_average)[-1]


def back_test(sid, start_backtest_date: datetime, n_daily_average=5, test_day_list: List = [30, 60, 120, 180, 360]):
    # sid = '2330'
    # start_backtest_date = datetime(year=2023, month=9, day=18)
    # test_day_list: List = [30, 60, 120, 180, 360]
    # n_daily_average = 5
    # 不可早於今天
    assert start_backtest_date <= datetime.today(), f'start_backtest_date不可早於今天'
    n_daily_average = 5
    # 確認時間格式
    assert isinstance(start_backtest_date, datetime)

    my_stock = MyStock(sid, initial_fetch=False)
    start_stock_price = my_stock.get_target_date_n_daily_average_price(start_backtest_date, n_daily_average)
    print(f'起始日期: {start_backtest_date}, 起始股價: {start_stock_price}, N日均價: {n_daily_average}日')
    for i in test_day_list:
        test_date = start_backtest_date + timedelta(days=i)
        if test_date > datetime.today():
            print(f'測試日期: {test_date}超過今天，無法進行測試')
            continue
        test_stock_price = my_stock.get_target_date_n_daily_average_price(test_date, n_daily_average)
        print(
            f"測試日期: {test_date}(經過{(test_date - start_backtest_date).days}天), "
            f"測試股價: {test_stock_price}, "
            f"起始股價: {start_stock_price}, "
            f"漲跌幅: {(test_stock_price / start_stock_price - 1) * 100:.2f}%,"
            f"年均報酬率: {((test_stock_price / start_stock_price) ** (365 / i) - 1) * 100:.2f}%")


if __name__ == '__main__':
    back_test('2330', datetime(year=2023, month=9, day=18), 5)

###

# stock.a = fetch_from1
#
# stock = Stock('2330')
# len(stock.price)
# ma_p = stock.moving_average(stock.price, 1)
# stock.__class__.
# stock.fetch_from(2020, 1)
# stock.fetcher.fetch(2020, 1, stock.sid)


# # 處理該月份資料
# def to_dict(date_data):
#     return {key: val for key, val in zip(date_data._fields, date_data)}
#
# start_backtest_month_data_dict = {tmp_date_data.date: to_dict(tmp_date_data) for tmp_date_data in
#                                   start_backtest_month_data}
