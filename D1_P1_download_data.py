import re
import bs4
import pandas as pd
import requests

from config import upload_date, date_format
from db_connect import conn


def get_ptt_article_list(ptt_url, page=''):
    # ptt_url = "https://www.ptt.cc/bbs/Stock/index"
    url = f"{ptt_url}{page}.html"
    my_headers = {'cookie': 'over18=1;'}
    response = requests.get(url, headers=my_headers)
    if response.ok:
        soup = bs4.BeautifulSoup(response.text, "html.parser")

        titles = soup.find_all('div', 'title')
        tmp_title_dict = [{'page': page, 'AID': tag.contents[1].attrs['href'], 'title': tag.text.strip()} for tag in
                          titles if '刪除' not in tag.text]
        print(f"Get article list '{url}' success，get total {len(tmp_title_dict)} articles ids")
        return tmp_title_dict
    else:
        print(f"Get article list '{url}' fail")


def get_ptt_article_detail(page, article_AID):  # article_AID = '/bbs/Stock/M.1709714209.A.657.html'
    article_url = f"https://www.ptt.cc" + article_AID
    my_headers = {'cookie': 'over18=1;'}
    response = requests.get(article_url, headers=my_headers)
    try:
        if response.ok:
            print(f"Get page:{page} article from '{article_url}'success. Start data process...")
            res = dict()
            res['AID'] = article_AID
            soup = bs4.BeautifulSoup(response.text, "html.parser")

            ## PTT 上方4個欄位
            header = soup.find_all('span', 'article-meta-value')
            # 作者
            res['author'] = header[0].text
            # 看版
            res['board'] = header[1].text
            # 標題
            res['title'] = header[2].text
            # 日期
            res['date'] = header[3].text

            ## 查找所有html 元素 抓出內容
            main_container = soup.find(id='main-container')
            # 把所有文字都抓出來
            all_text = main_container.text
            # 把整個內容切割透過 "-- " 切割成2個陣列
            pre_text = all_text.split('--')[0]

            # 把每段文字 根據 '\n' 切開
            texts = pre_text.split('\n')
            # 如果你爬多篇你會發現
            contents = texts[2:]
            # 內容
            res['content'] = '\n'.join(contents)
            print(f"Process page:{page} 文章'{article_url}'success.")
            return res
        else:
            print(f"Get page:{page} article from '{article_url}'fail")
            # 紀錄該AID抓取失敗，避免下次重複處理
            fail_df = pd.DataFrame(
                [{'page': page, 'AID': article_AID, 'upload_date': upload_date, 'error': '抓取文章失敗'}])
            fail_df.to_sql('ppt_article_fail_download_aid', conn, if_exists='append', index=False)
    except BaseException:
        print(f"Process page:{page} 文章'{article_url}' occur unexpected error.")
        # 紀錄該AID抓取失敗，避免下次重複處理
        fail_df = pd.DataFrame(
            [{'page': page, 'AID': article_AID, 'upload_date': upload_date, 'error': '處理文章時發生未預期錯誤'}])
        fail_df.to_sql('ppt_article_fail_download_aid', conn, if_exists='append', index=False)


def get_ptt_newest_page_index(ptt_url):
    # ptt_url = f"https://www.ptt.cc/bbs/Stock/index.html"
    my_headers = {'cookie': 'over18=1;'}
    response = requests.get(ptt_url, headers=my_headers)
    soup = bs4.BeautifulSoup(response.text, "html.parser")

    res = [tag.attrs['href'] for tag in soup.find_all('a', 'btn wide') if tag.contents == ['‹ 上頁']]

    pattern = r"index(\d+)\.html"
    match = re.search(pattern, res[0])
    return str(int(match.group(1)) + 1)


if __name__ == '__main__':

    print('Start downloaded page list and ids')
    # check newest page
    newest_page = int(get_ptt_newest_page_index(ptt_url=f"https://www.ptt.cc/bbs/Stock/index.html"))
    max_n_page = 300

    ## check header have downloaded

    if 'ppt_article_ids' in \
            pd.read_sql("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'ppt_article_ids';", conn)[
                'name'].values:
        last_downloaded_page = int(pd.read_sql("select max(page) max_page from ppt_article_ids", conn)['max_page'][0])
        downloaded_AID_set = set(pd.read_sql("select AID from ppt_article_ids", conn)['AID'])
    else:
        last_downloaded_page = 0
        downloaded_AID_set = {}

    ## download ids
    title_df_list = []
    print(f'    Last_downloaded_page:{last_downloaded_page}')
    start_download_page = max(last_downloaded_page, newest_page - max_n_page)
    # start_download_page = newest_page - n_page
    print(f'    Start_download_page:{start_download_page}')
    print(f'    Newest_page:{newest_page}')
    for tmp_page in range(start_download_page, newest_page + 1):
        title_dict = get_ptt_article_list(ptt_url=f"https://www.ptt.cc/bbs/Stock/index", page=str(tmp_page))
        title_df = pd.DataFrame(title_dict)
        title_df_list.append(title_df)
    all_title_df = pd.concat(title_df_list, ignore_index=True)
    all_title_df = all_title_df[~all_title_df['AID'].isin(downloaded_AID_set)]
    all_title_df['upload_date'] = upload_date

    # save to db
    # all_title_df = pd.read_sql('select * from header',conn)
    all_title_df.to_sql('ppt_article_ids', conn, if_exists='append', index=False)
    # all_title_df.to_sql('ppt_article_ids', conn, if_exists='replace', index=False)
    conn.commit()
    print('End downloaded page list and ids')

    print('Start download article detail by ppt_article_ids')
    ## download text by ppt_article_ids
    # ppt_article_ids from db
    get_ids_sql = "select * from ppt_article_ids where 0=0"
    # 若已經有ppt_article_details此table代表可能抓過資料了，為了避免抓到重複的內文，須對AID做篩選
    if 'ppt_article_details' in \
            pd.read_sql("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'ppt_article_details';", conn)[
                'name'].values:
        get_ids_sql += " and AID not in (select AID from ppt_article_details)"
    # 若已經有ppt_article_fail_download_aid此table代表可能抓過資料了，且抓取失敗，為了避免抓到重複的內文，須對AID做篩選
    if 'ppt_article_fail_download_aid' in pd.read_sql(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'ppt_article_fail_download_aid';", conn)[
        'name'].values:
        get_ids_sql += " and AID not in (select AID from ppt_article_fail_download_aid)"

    all_title_df = pd.read_sql(get_ids_sql, conn)

    all_title_df.sort_values(by='page', inplace=True, ignore_index=True)  # 依照page排序，早的資料先抓

    # page+AID的tuple
    AID_tuple_list = all_title_df.apply(lambda x: (x['page'], x['AID']), axis=1).tolist()
    print(f' Total have {len(AID_tuple_list)}   AID to download.')
    detail_df_list = []
    for i in range(len(AID_tuple_list)):
        AID_tuple = AID_tuple_list[i]
        if i % 100 == 0:
            print(f'Already processed {i}/{len(AID_tuple_list)} articles.')
        detail_df_list.append(get_ptt_article_detail(page=AID_tuple[0], article_AID=AID_tuple[1]))
        if (i % 2000 == 0) or (i == len(AID_tuple_list) - 1):
            # 每兩千儲存一次文章
            # preprocess data
            # all_detail_df = pd.read_sql("select * from ppt_article_details", conn)
            all_detail_df = pd.DataFrame([x for x in detail_df_list if x is not None])

            if not all_detail_df.empty:

                # spilt author
                auther_pattern = r"(.*?)\((.*)\)"
                tmp_series = all_detail_df['author'].apply(lambda x: re.search(auther_pattern, x))
                all_detail_df['author0'] = tmp_series.apply(lambda x: x.groups()[0] if x is not None else None)
                all_detail_df['author1'] = tmp_series.apply(lambda x: x.groups()[1] if x is not None else None)

                # spilt title category
                title_pattern = r".*\[(.*)\].*"
                all_detail_df['category'] = all_detail_df['title'].apply(lambda x: re.search(title_pattern, x))
                all_detail_df['category'] = all_detail_df['category'].apply(
                    lambda x: x.groups()[0] if x is not None else None)

                # check is Re
                re_pattern = r"Re: \[(.*)\].*"
                all_detail_df['is_re'] = all_detail_df['title'].apply(lambda x: bool(re.match(re_pattern, x)))

                # format date
                all_detail_df['date_format'] = all_detail_df['date'].apply(
                    lambda x: pd.to_datetime(x, format='%a %b %d %H:%M:%S %Y').strftime(date_format))

                # url
                all_detail_df['url'] = all_detail_df['AID'].apply(lambda x: "https://www.ptt.cc" + x)

                # upload date
                all_detail_df['upload_date'] = upload_date

                print(f'Save {len(all_detail_df)} articles to ppt_article_details table.')
                all_detail_df.to_sql('ppt_article_details', conn, if_exists='append', index=False)
                # all_detail_df.to_sql('ppt_article_details', conn, if_exists='replace', index=False)
                conn.commit()
            else:
                print('There is no article.')
            # 清空
            detail_df_list = []

    print('End downloaded text by ppt_article_ids')
