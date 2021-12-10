'''
1. 신규 데이터 인서트
    star_ko_data -> star_album_genre

2. 바이브 키 값, 데이터 수집 (전체 or 신규)



'''


import pandas as pd
from bs4 import BeautifulSoup
import requests
import re
import datetime
from difflib import SequenceMatcher
import pymysql
from sqlalchemy import create_engine
from tqdm import tqdm
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from tqdm import tqdm
import time

def db_connection(host_name='ds'):
    host_url = "db.ds.mycelebs.com"
    user_nm =
    passwd =
    port_num = 3306
    db_name = "star_ko"
    conn = pymysql.connect(host=host_url, user=user_nm, passwd=passwd, port = port_num, charset='utf8',
                           db=db_name, cursorclass=pymysql.cursors.DictCursor)
    # cursorclass=pymysql.cursors.DictCursor 추가 -> 데이터프레임으로 다루기 쉽게 딕셔너리 형태로 결과 반환해줌, cursor는 튜플형태
    # db=db,
    engine = create_engine(f'mysql+pymysql://{user_nm}:{passwd}@{host_url}:{port_num}/{db_name}?charset=utf8mb4')
    return conn, engine


# donut -> db 신규 데이터 insert
def insert() :
    conn, engine = db_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    qry = 'SELECT MAX(series_id) FROM star_album_genre2'
    maxID = pd.read_sql(qry, conn)
    maxID = maxID.iloc[0, 0]


    qry = f"SELECT series_id, vibe_people_id, cd_name FROM star_ko_data WHERE series_id > {maxID} and cd_is_use=1"
    cnt = pd.read_sql(qry, conn)

    if len(cnt) == 0 :
        pass

    else :
        insertQRY = f"INSERT INTO star_album_genre2 (series_id, vibeCd, name) SELECT series_id, vibe_people_id, cd_name FROM star_ko_data WHERE series_id > {maxID} and cd_is_use=1"
        cursor.execute(insertQRY)
        conn.commit()

        print(len(cnt), "개 신규 데이터 insert 완료" )

    conn.close()


def crawling(status) :
    conn, engine = db_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    if status == 'new' :
        pass

    elif status == 'tot' :
        qry = "SELECT * FROM star_album_genre2 WHERE ifnull(vibeCd, '') != -1 and (title is NULL or track_list is NULL or vibeCd is NULL)"
        ex = pd.read_sql(qry, conn)

        print("[INFO]", len(ex), "개 개체의 vibe 수집을 시작합니다 ... ")

        chrome_options = webdriver.ChromeOptions()
        # chrome_options.add_argument('--headless')
        driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)

        for i, row in tqdm(ex.iterrows(), total=ex.shape[0]):

            album_url = []
            genre_list = []
            track_list = []
            title_song_list = []

            series_id = row['series_id']
            vibeCd = row['vibeCd']
            name = row['name']

            print("\n")
            print(series_id, "-", name, "crawling ... ")

            if pd.isnull(vibeCd) or vibeCd == '' :

                qry = f'SELECT * FROM star_ko_profile_url WHERE series_id={series_id}'
                profileData = pd.read_sql(qry, con=conn)

                if len(profileData) == 0 :
                    pass

                else :

                    people_id = profileData['naver_people_id'][0]

                    if people_id == 'x' or people_id == '' or pd.isnull(people_id) :
                        pass

                    else :

                        naver_url = f'https://search.naver.com/search.naver?where=nexearch&sm=tab_etc&mra=bjky&x_csa=%7B%22fromUi%22%3A%22kb%22%7D&pkid=1&os={people_id}&qvt=0&query={name}'
                        naver_url_ = f'https://search.naver.com/search.naver?where=nexearch&sm=tab_etc&mra=bjky&x_csa=%7B%22workType%22%3A%22album%22%2C%22fromUi%22%3A%22kb%22%7D&pkid=1&os={people_id}&qvt=0&query={name}' + ' 앨범'

                        artist_url = album_url2 = vibeCd = albumName = vibeLink = artist = ''

                        time.sleep(0.5)

                        naver_html = requests.get(naver_url).text
                        naver_soup = BeautifulSoup(naver_html, 'html.parser')
                        time.sleep(0.5)

                        nameList = []
                        nameList.append(name)

                        try:
                            true_name = naver_soup.select('strong[class="_text"]')[0].text  # 네이버 공식 이름
                            nameList.append(true_name)

                            sub_title = naver_soup.select('div[class="sub_title first_elss"]')[0].select(
                                'span[class=txt]')  # 본명/영어이름
                            if len(sub_title) == 2:
                                true_eng_name = sub_title[0].text
                                true_eng_name = true_eng_name.split(", ")
                                nameList = nameList + true_eng_name
                        except:
                            pass

                        naver_html_ = requests.get(naver_url_).text
                        naver_soup_ = BeautifulSoup(naver_html_, 'html.parser')

                        try :
                            panel_album = naver_soup_.select('div[data-name=album]')
                            album_list = panel_album[0].select('div[class="info_box"]')

                            for r in range(len(album_list)):
                                albumName_ = album_list[r].find_all("a", {"class": "_text"})[0].text
                                album_url_ = album_list[r].find_all("a", {"class": "_text"})[0].get('href')
                                artist_ = album_list[r].find_all("dl", {"class": "rel_info txt_4"})[0].select('dd')[0].text
                                artist_ = re.sub(r'\([^)]*\)', '', artist_)  # 괄호, 괄호 안 문자 삭제 ex. 온유(리더), 종현(메인보컬), ...
                                artist_ = artist_.rstrip()

                                if ',' in artist_:  # 듀엣 제거
                                    pass
                                elif artist_ in nameList:
                                    albumName = albumName_
                                    album_url2 = album_url_
                                    artist = artist_

                                    driver.get(album_url2)
                                    time.sleep(3)

                                    try:
                                        driver.find_element_by_xpath('//*[@id="app"]/div[2]/div/div/a[2]').click()
                                    except:
                                        pass

                                    html = driver.page_source
                                    soup = BeautifulSoup(html, 'html.parser')

                                    vibeCd = soup.select('a[class=link_sub_title]')[0].get('href')
                                    vibeCd = re.findall("\d+", vibeCd)[0]

                                    updateQRY1 = f"UPDATE star_album_genre2 SET vibeCd={vibeCd} WHERE series_id={series_id}"
                                    cursor.execute(updateQRY1)
                                    conn.commit()

                                    break

                        except:
                            updateQRY2 = f"UPDATE star_album_genre2 SET vibeCd=-1 WHERE series_id={series_id}"
                            cursor.execute(updateQRY2)
                            conn.commit()


                        print("(", series_id, "-", name,  "vibeCd insert DONE )")

            qry = f"SELECT * from star_album_genre2 WHERE series_id={series_id}"
            result = pd.read_sql(qry, conn)
            try:
                before_title = result['title'][0]
                before_track = result['track_list'][0]
                before_new_albumName = result['new_albumName'][0]
                before_release_date = result['release_date'][0]
                before_genre = result['genre'][0]
            except:
                before_title = ''
                before_track = ''
                before_new_albumName = ''
                before_release_date = ''
                before_genre = ''

            try :
                vibe_url = 'https://vibe.naver.com/artist/' + str(int(vibeCd)) + '/albums'

                driver.get(vibe_url)
                time.sleep(1)
                try :
                    driver.find_element_by_xpath('//*[@id="app"]/div[2]/div/div/a[2]').click()  # 팝업창 끄기
                except :
                    pass

                while True:
                    try:
                        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")  # 스크롤 끝까지 내리기
                        time.sleep(1)

                        driver.find_element_by_xpath('//*[@id="content"]/div/div[3]/div/a').click()  # 더보기 클릭
                        time.sleep(1)
                    except:
                        break

                vibe_html = driver.page_source
                vibe_soup = BeautifulSoup(vibe_html, 'html.parser')

                # 앨범 리스트
                album_list = vibe_soup.select('a[class=title]')

                if album_list == []:
                    print("[INFO]", series_id, "-", name, "전체 앨범 없음 ")

                else :

                    for a in album_list:
                        href = a.get('href')
                        href = 'https://vibe.naver.com' + href
                        album_url.append(href)

                    # 최신 앨범명 & 발매 날짜
                    newURL = album_url[0]

                    driver.get(newURL)
                    html = driver.page_source
                    soup = BeautifulSoup(html, 'html.parser')

                    # 앨범명
                    title = soup.select('span[class=title]')[0].text
                    title = re.sub("\'", "", title)  # 특수문자 제거
                    # 발매 날짜
                    relese_date = soup.select('span[class=item]')[0].text
                    relese_date = relese_date.replace(' ', '')

                    # 모든 장르 & 타이틀 & 수록곡
                    for g in album_url:
                        driver.get(g)
                        html_ = driver.page_source
                        soup_ = BeautifulSoup(html_, 'html.parser')

                        genre = soup_.select('span[class=item]')[1].text
                        genre = genre.replace(' ', '')

                        genre_list.append(genre)

                        title_badge_wrap = soup_.select('div[class=title_badge_wrap]')

                        for song in range(len(title_badge_wrap)):

                            track = title_badge_wrap[song].find('a').text
                            track = re.sub("\'", "", track)  # 특수문자 제거
                            track_list.append(track)

                            try:
                                title_badge_wrap_ = title_badge_wrap[song].select('span[class=badge_title]')[0].text
                                if title_badge_wrap_ == '타이틀':
                                    title_song = title_badge_wrap[song].find('a').text
                                    title_song = re.sub("\'", "", title_song)  # 특수문자 제거
                                    title_song_list.append(title_song)
                            except:
                                pass

                    genre_list = list(set(genre_list))  # 중복 제거
                    track_list = list(reversed(track_list))
                    title_song_list = list(reversed(title_song_list))

                    # 사업자 등록 번호 제거
                    for l in genre_list[::-1]:
                        if l.find('사업자') > -1:
                            genre_list.remove(l)

                    # 리스트 문자열로 합치기
                    genre_str = '|'.join(genre_list)
                    track_list_str = '|'.join(track_list)
                    title_song_str = '|'.join(title_song_list)

                    if before_title != title_song_str or before_new_albumName != title or before_release_date != relese_date or before_genre != genre or before_track != track_list_str :
                        updateQRY3 = f"UPDATE star_album_genre2 SET title='{title_song_str}', track_list='{track_list_str}', new_albumName='{title}', release_date='{relese_date}', genre='{genre_str}'  WHERE series_id={series_id}"
                        cursor.execute(updateQRY3)
                        conn.commit()

                    print("[INFO]", series_id, "-", name, "crawling DONE ")

            except Exception as e :
                print("error ! ", e)


        conn.close()
        driver.close()



if __name__ == '__main__':
    insert()
    crawling('tot')