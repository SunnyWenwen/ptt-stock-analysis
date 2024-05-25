if __name__ == '__main__':
    """
    分析0050股價與PTT文章數的相關性。
    """

    import collections
    from db_connect import conn
    import pandas as pd

    # 抓取資料
    # 計算每日文章數，需有跑過download_data.py，才會有ppt資料
    df_ptt = pd.read_sql("select category,date_format date from ppt_article_details", conn)
    df_ptt['date'] = pd.to_datetime(df_ptt['date'])
    df_ptt['date'] = df_ptt['date'].dt.date
    df_ptt_ct = pd.DataFrame(collections.Counter(df_ptt['date']), index=['count']).T.reset_index()
    df_ptt_ct.rename(columns={'index': 'date'}, inplace=True)

    # 抓取每天的大盤漲跌
    from import_tw_stock_analysis import MyStock

    stock = MyStock('0050').fetch_from(2023, 1)
    df_0050 = pd.DataFrame(stock)[['date', 'close', 'change']]
    df_0050['date'] = pd.to_datetime(df_0050['date']).dt.date

    # 合併兩資料
    df = pd.merge(df_ptt_ct, df_0050, on='date', how='inner')
    # 排序
    df.sort_values(by='date', inplace=True)

    # 計算相關係數
    print(df['count'].corr(df['close']))
    print(df['count'].corr(df['change']))
    print(df['count'].corr(abs(df['change'])))

    import matplotlib as plt

    plt.use('TkAgg')
    import matplotlib.pyplot as plt

    # 圖1 股價vs數量
    fig, ax = plt.subplots()
    ax.plot(df['date'], df['count'], label='文章數')
    ax2 = ax.twinx()
    ax2.plot(df['date'], df['close'], label='大盤', color='r')

    # 圖2 股價vs變化輛
    fig, ax = plt.subplots()
    ax.plot(df['date'], df['count'], label='文章數')
    ax2 = ax.twinx()
    ax2.plot(df['date'], abs(df['change']), label='大盤', color='r')

    # 圖3 股價vs變化輛絕對值
    fig, ax = plt.subplots()
    ax.plot(df['date'], df['count'], label='文章數')
    ax2 = ax.twinx()
    ax2.plot(df['date'], abs(df['change']), label='大盤', color='r')

    # 不要看總數，看特定類情的文章與股價的相關性
    all_cate = df_ptt['category'].unique()
    for cate in all_cate:
        df_ptt_ct_tmp = df_ptt[df_ptt['category'] == cate]
        df_ptt_ct_tmp = pd.DataFrame(collections.Counter(df_ptt_ct_tmp['date']), index=['count']).T.reset_index()
        df_ptt_ct_tmp.rename(columns={'index': 'date'}, inplace=True)
        df = pd.merge(df_ptt_ct_tmp, df_0050, on='date', how='inner')
        df.sort_values(by='date', inplace=True)
        if len(df) > 2 and set(df['count']) != {1}:
            print(f'{cate}文章數與大盤的相關性')
            print(f"    與收盤相關{round(df['count'].corr(df['close']), 2)}")
            print(f"    與價格變動相關{round(df['count'].corr(df['change']), 2)}")
