import asyncio
import re

import aiohttp
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
        # 有些文張標題格式會跑掉,所以contents內有要有兩個以上的元素才抓
        tmp_title_dict = [{'page': page, 'AID': tag.contents[1].attrs['href'], 'title': tag.text.strip()} for tag in
                          titles if '刪除' not in tag.text and len(tag.contents) > 1]
        print(f"Get article list '{url}' success，get total {len(tmp_title_dict)} articles ids")
        return tmp_title_dict
    else:
        print(f"Get article list '{url}' fail")


def get_ptt_article_detail(page, article_AID):  # article_AID = '/bbs/Stock/M.1582026074.A.570.html'
    """
    舊的運行方式，處理單一article_AID用
    現在已改為使用async批量處理，不再使用此function
    留著用來debug&測試
    :param page:
    :param article_AID:
    :return:
    """
    article_url = f"https://www.ptt.cc" + article_AID
    my_headers = {'cookie': 'over18=1;'}
    response = requests.get(article_url, headers=my_headers)
    text = response.text if response.ok else False
    tmp_response = {'page': page, 'article_AID': article_AID, 'article_url': article_url, 'text': text}
    return process_response(tmp_response)


def get_ptt_newest_page_index(ptt_url):
    # ptt_url = f"https://www.ptt.cc/bbs/Stock/index.html"
    my_headers = {'cookie': 'over18=1;'}
    response = requests.get(ptt_url, headers=my_headers)
    soup = bs4.BeautifulSoup(response.text, "html.parser")

    res = [tag.attrs['href'] for tag in soup.find_all('a', 'btn wide') if tag.contents == ['‹ 上頁']]

    pattern = r"index(\d+)\.html"
    match = re.search(pattern, res[0])
    return str(int(match.group(1)) + 1)


async def fetch(page, article_AID):
    article_url = f"https://www.ptt.cc" + article_AID
    async with aiohttp.ClientSession() as session:
        async with session.get(article_url, headers={'cookie': 'over18=1;'}) as response:
            if response.status == 200:  # 只处理成功的响应
                text = await response.text()  # 假设响应内容是 text
                return {'page': page, 'article_AID': article_AID, 'article_url': article_url, 'text': text}
            else:
                return {'page': page, 'article_AID': article_AID, 'article_url': article_url, 'text': False}


async def get_batch_response(tmp_AID_tuple_list):
    tasks = [asyncio.create_task(fetch(page, article_AID)) for page, article_AID in tmp_AID_tuple_list]
    responses = await asyncio.gather(*tasks)
    return responses


def process_response(response):
    try:
        if response['text'] is not False:
            res = dict()
            res['AID'] = response['article_AID']
            text = response['text']
            soup = bs4.BeautifulSoup(text, "html.parser")
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
            # 日期要符合其格式，否則算失敗
            pd.to_datetime(res['date'], format='%a %b %d %H:%M:%S %Y')

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
            print(f"Process page:{response['page']} 文章'{response['article_url']}'success.")
            return res
        else:
            fail_df = pd.DataFrame(
                [{'page': response['page'], 'AID': response['article_AID'], 'upload_date': upload_date,
                  'error': '抓取文章失敗'}])
            print(f"Get page:{response['page']} article from '{response['article_url']}'fail")
            fail_df.to_sql('ppt_article_fail_download_aid', conn, if_exists='append', index=False)
    except BaseException:
        print(f"Process page:{response['page']} 文章'{response['article_url']}' occur unexpected error.")
        # 紀錄該AID抓取失敗，避免下次重複處理
        fail_df = pd.DataFrame(
            [{'page': response['page'], 'AID': response['article_AID'], 'upload_date': upload_date,
              'error': '處理文章時發生未預期錯誤'}])
        fail_df.to_sql('ppt_article_fail_download_aid', conn, if_exists='append', index=False)


if __name__ == '__main__':

    print('Start downloaded page list and ids')
    # check newest page
    newest_page = int(get_ptt_newest_page_index(ptt_url=f"https://www.ptt.cc/bbs/Stock/index.html"))
    max_n_page = 300

    ## check header have downloaded

    if 'ppt_article_ids' in \
            pd.read_sql("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'ppt_article_ids';", conn)[
                'name'].values:
        last_downloaded_page = int(
            pd.read_sql("select MAX(CAST(page AS INTEGER)) max_page from ppt_article_ids", conn)['max_page'][0])
        # oldest_downloaded_page = int(pd.read_sql("select min(page) min_page from ppt_article_ids", conn)['min_page'][0])
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

    batch_size = 500

    for i in range(0, len(AID_tuple_list), batch_size):  # i=0
        print(f'Start processed {i}~{i + batch_size}/{len(AID_tuple_list)} articles.')
        tmp_AID_tuple_list = AID_tuple_list[i:i + batch_size]
        responses = asyncio.run(get_batch_response(tmp_AID_tuple_list))
        detail_df_list = list(map(process_response, responses))
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

        print(f'End processed {i}~{i + batch_size}/{len(AID_tuple_list)} articles.')

    print('End downloaded text by ppt_article_ids')
