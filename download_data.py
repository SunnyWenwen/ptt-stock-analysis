import re
import bs4
import pandas as pd
import requests

from const import upload_date, date_format
from db_connect import conn


def get_ptt_article_list(ptt_url, page=''):
    # ptt_url = "https://www.ptt.cc/bbs/Stock/index"
    url = f"{ptt_url}{page}.html"
    my_headers = {'cookie': 'over18=1;'}
    response = requests.get(url, headers=my_headers)
    if response.ok:
        soup = bs4.BeautifulSoup(response.text, "html.parser")

        titles = soup.find_all('div', 'title')
        title_dict = [{'page': page, 'AID': tag.contents[1].attrs['href'], 'title': tag.text.strip()} for tag in titles
                      if '刪除' not in tag.text]
        print(f"抓取文章列表'{url}'成功")
        return title_dict
    else:
        print(f"抓取文章列表'{url}'失敗")


def get_ptt_article_info(article_AID):
    article_url = f"https://www.ptt.cc" + article_AID
    my_headers = {'cookie': 'over18=1;'}
    response = requests.get(article_url, headers=my_headers)
    try:
        if response.ok:
            print(f"抓取文章'{article_url}'成功")
            soup = bs4.BeautifulSoup(response.text, "html.parser")
            res = dict()
            res['AID'] = article_AID

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
            return res
        else:
            print(f"抓取文章'{article_url}'失敗")
    except BaseException:
        print(f"抓取文章'{article_url}'進行處理時發生未預期錯誤")


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

    print('Start downloaded page')
    # check newest page
    newest_page = int(get_ptt_newest_page_index(ptt_url=f"https://www.ptt.cc/bbs/Stock/index.html"))
    max_n_page = 300

    ## check header have downloaded

    if 'header' in pd.read_sql("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'header';", conn)[
        'name'].values:
        last_downloaded_page = int(pd.read_sql("select max(page) max_page from header", conn)['max_page'][0])
        downloaded_AID_set = set(pd.read_sql("select AID from header", conn)['AID'])
    else:
        last_downloaded_page = 0
        downloaded_AID_set = {}

    ## download header
    title_df_list = []
    print(f'    last_downloaded_page:{last_downloaded_page}')
    start_download_page = max(last_downloaded_page, newest_page - max_n_page)
    # start_download_page = newest_page - n_page
    print(f'    start_download_page:{start_download_page}')
    print(f'    newest_page:{newest_page}')
    for tmp_page in range(start_download_page, newest_page + 1):
        title_dict = get_ptt_article_list(ptt_url=f"https://www.ptt.cc/bbs/Stock/index", page=str(tmp_page))
        title_df = pd.DataFrame(title_dict)
        title_df_list.append(title_df)
    all_title_df = pd.concat(title_df_list, ignore_index=True)
    all_title_df = all_title_df[~all_title_df['AID'].isin(downloaded_AID_set)]
    all_title_df['upload_date'] = upload_date

    # save to db
    # all_title_df = pd.read_sql('select * from header',conn)
    all_title_df.to_sql('header', conn, if_exists='append', index=False)
    # all_title_df.to_sql('header', conn, if_exists='replace', index=False)
    conn.commit()
    print('End downloaded page')

    print('Start downloaded text by header')
    ## download text by header
    # header from db
    if 'info' in pd.read_sql("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'info';", conn)[
        'name'].values:
        # 若已經有info此table代表可能抓過資料了，為了避免抓到重複的內文，須對AID做篩選
        all_title_df = pd.read_sql("select * from header where AID not in (select AID from info)", conn)
    else:
        all_title_df = pd.read_sql("select * from header ", conn)

    AID_list = all_title_df['AID']
    print(f' 共有{len(AID_list)}篇文章要下載')
    info_df_list = []
    for i in range(len(AID_list)):
        if i % 100 == 0:
            print(f'已處理{i}個AID')
        info_df_list.append(get_ptt_article_info(article_AID=AID_list[i]))
        if (i % 2000 == 0) or (i == len(AID_list) - 1):
            # 每兩千儲存一次文章
            # preprocess data
            # all_info_df = pd.read_sql("select * from info", conn)
            all_info_df = pd.DataFrame([x for x in info_df_list if x is not None])

            if not all_info_df.empty:

                # spilt author
                auther_pattern = r"(.*?)\((.*)\)"
                tmp_series = all_info_df['author'].apply(lambda x: re.search(auther_pattern, x))
                all_info_df['author0'] = tmp_series.apply(lambda x: x.groups()[0] if x is not None else None)
                all_info_df['author1'] = tmp_series.apply(lambda x: x.groups()[1] if x is not None else None)

                # spilt title category
                title_pattern = r".*\[(.*)\].*"
                all_info_df['category'] = all_info_df['title'].apply(lambda x: re.search(title_pattern, x))
                all_info_df['category'] = all_info_df['category'].apply(
                    lambda x: x.groups()[0] if x is not None else None)

                # check is Re
                re_pattern = r"Re: \[(.*)\].*"
                all_info_df['is_re'] = all_info_df['title'].apply(lambda x: bool(re.match(re_pattern, x)))

                # format date
                all_info_df['date_format'] = all_info_df['date'].apply(
                    lambda x: pd.to_datetime(x, format='%a %b %d %H:%M:%S %Y').strftime(date_format))

                # url
                all_info_df['url'] = all_info_df['AID'].apply(lambda x: "https://www.ptt.cc" + x)

                # upload date
                all_info_df['upload_date'] = upload_date

                print(f' 存入info {len(all_info_df)}篇文章')
                all_info_df.to_sql('info', conn, if_exists='append', index=False)
                # all_info_df.to_sql('info', conn, if_exists='replace', index=False)
                conn.commit()
            else:
                print('一篇文章都沒有。')
            # 清空
            info_df_list = []

    print('End downloaded text by header')
