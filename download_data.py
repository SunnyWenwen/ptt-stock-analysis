import re

import bs4
import pandas as pd
import requests

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
        print(f"抓取{url}成功")
        return title_dict
    else:
        print(f"抓取{url}失敗")


def get_ptt_article_info(article_AID):
    article_url = f"https://www.ptt.cc" + article_AID
    my_headers = {'cookie': 'over18=1;'}
    response = requests.get(article_url, headers=my_headers)
    try:
        if response.ok:
            print(f"抓取{article_url}成功")
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
            print(f"抓取{article_url}失敗")
    except BaseException:
        print(f'處理{article_url}資料時發生未預期錯誤')


def get_ptt_newest_page_index(ptt_url):
    # ptt_url = f"https://www.ptt.cc/bbs/Stock/index.html"
    my_headers = {'cookie': 'over18=1;'}
    response = requests.get(ptt_url, headers=my_headers)
    soup = bs4.BeautifulSoup(response.text, "html.parser")

    res = [tag.attrs['href'] for tag in soup.find_all('a', 'btn wide') if tag.contents == ['‹ 上頁']]

    pattern = r"index(\d+)\.html"
    match = re.search(pattern, res[0])
    return str(int(match.group(1)) + 1)


if __name__ == 'main':
    # download head
    newest_page = int(get_ptt_newest_page_index(ptt_url=f"https://www.ptt.cc/bbs/Stock/index.html"))
    n_page = 100
    res_list = []
    for tmp_page in range(newest_page - n_page, newest_page):
        title_dict = get_ptt_article_list(ptt_url=f"https://www.ptt.cc/bbs/Stock/index", page=str(tmp_page))
        title_df = pd.DataFrame(title_dict)
        res_list.append(title_df)
    all_res = pd.concat(res_list, ignore_index=True)

    all_res.to_sql('header', conn, if_exists='append', index=False)
    conn.commit()

    # download text
    AID_list = all_res['AID']
    res_list = []
    for i in range(len(AID_list)):
        # i = 0
        res_list.append(get_ptt_article_info(article_AID=AID_list[i]))

    all_res = pd.DataFrame([x for x in res_list if x is not None])
    all_res.to_sql('info', conn, if_exists='append', index=False)
    conn.commit()
