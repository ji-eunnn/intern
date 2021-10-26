'''

목표 : playDB.co.kr 에서 각종 공연 정보를 크롤링하고 출연하는 배우들과 celeb 스타를 매핑, celeb에 없는 스타는 신규로 인물등록 진행

프로세스 :
-> playDB에 올라와있는 뮤지컬, 연극, 콘서트 전부 크롤링 - crawling()
-> 전날 데이터(DB.xlsx)와 비교해서 새로 업데이트된 공연 정보만 추출 - concat()
-> star_ko_performance_connection 에 이미 매핑되어있는 개체들 먼저 1차 매핑 - mapping_1()
-> star_ko_performance_connection 에 없는 개체들 2차 수기 매핑 - mapping_2()
-> db에 insert - make_star_ko_performance(), make_performance_actor_name(), db_update()
-> performance_actor_name 의 경우, truncate 후 insert 해야 함 - db_trunc_insert()
-> 매핑 완료된 개체들 보고 - report()
-> 매핑 안된, 신규로 등록해야 하는 개체들 시트에 맞게 데이터프레임 만들어주고 - produce_dataframe()
-> playDB에서 제공하는 출연작품과 네이버 프로필에서 제공하는 작품활동을 비교하여 네이버 프로필 url을 찾아옴 - get_naver_url()
-> 인물등록이 완료되면, 등록한 series_id 시트에 붙여넣고 star_ko_performance_connection 에 insert - db_update2()


(21/10/26)
1. 자동 매핑 된 개체들 도넛에 playdb_id 값 넣어주도록 수정



'''


import pandas as pd
from datetime import date, timedelta
from bs4 import BeautifulSoup
import collections
import datetime
import requests
from tqdm import tqdm
import re
from itertools import count
import pymysql
from sqlalchemy import create_engine
from PIL import Image
from io import BytesIO
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
import time
import random


def db_connection(host_name='ds'):
    host_url = "db.ds.mycelebs.com"
    user_nm = "j_eungg"
    passwd = "wldms10529"
    port_num = 3306
    db_name = "star_ko"
    conn = pymysql.connect(host=host_url, user=user_nm, passwd=passwd, port=port_num, charset='utf8',
                           db=db_name, cursorclass=pymysql.cursors.DictCursor)
    # cursorclass=pymysql.cursors.DictCursor 추가 -> 데이터프레임으로 다루기 쉽게 딕셔너리 형태로 결과 반환해줌, cursor는 튜플형태
    # db=db,
    engine = create_engine(f'mysql+pymysql://{user_nm}:{passwd}@{host_url}:{port_num}/{db_name}?charset=utf8mb4')
    return conn, engine


def has_duplicates(seq):
    return len(seq) != len(set(tuple(sorted(d.items())) for d in seq))


def crawling(craw_genre) :
    if craw_genre == "뮤지컬" :
        url_ = 'http://www.playdb.co.kr/playdb/playdblist.asp?Page={}&sReqMainCategory=000001&sReqSubCategory=&sReqDistrict=&sReqTab=2&sPlayType=2&sStartYear=&sSelectType='
        genre_ = '뮤지컬'

    elif craw_genre == "연극" :
        url_ = 'http://www.playdb.co.kr/playdb/playdblist.asp?Page={}&sReqMainCategory=000002&sReqSubCategory=&sReqDistrict=&sReqTab=2&sPlayType=2&sStartYear=&sSelectType='
        genre_ = '연극'

    elif craw_genre == "콘서트" :
        url_ = 'http://www.playdb.co.kr/playdb/playdblist.asp?Page={}&sReqMainCategory=000003&sReqSubCategory=&sReqDistrict=&sReqTab=2&sPlayType=3&sStartYear=&sSelectType='
        genre_ = '콘서트'


    final_list = []
    base_url = 'http://www.playdb.co.kr/playdb/playdbDetail.asp?sReqPlayno={}'

    for page in count(1, 1):
        print(page)
        url1 = url_.format(page)
        html = requests.get(url1).text
        soup = BeautifulSoup(html, 'html.parser')

        getRange = len(soup.select('font > a'))

        for i in range(1, getRange):
            title = concertKey = img = ticket_url = date = location = actors = ''
            actor_list = []

            # 1. 제목
            title = soup.select('font > a')[i].text.rstrip()

            # 1.1 키값
            concertKey = soup.select('font > a')[i].get('onclick')
            concertKey = re.findall("\d+", concertKey)[0]

            # 6. 장르
            genre = genre_

            url2 = base_url.format(concertKey)
            html2 = requests.get(url2).text
            soup2 = BeautifulSoup(html2, 'html.parser')

            # 9. 이미지
            try:
                img = soup2.find('div', {'class': 'pddetail'}).find('img')['src']
            except:
                img = None

            detaillist = soup2.find('div', {'class': 'detaillist'})

            # 4. 예매 링크
            try:
                ticket_url = detaillist.select('p')[0].select('a')[0].get('href')
            except:
                ticket_url = None

            # 5. 플레이디비 링크
            reservation_url = url2

            # 7. 일시
            try:
                date = detaillist.find_all('td')[3].text.rstrip()
            except:
                date = None

            # 8. 장소
            try:
                location = detaillist.find_all('td')[5].text.rstrip()
            except:
                location = None

            # 2. 배우
            try:
                actors = detaillist.find_all('td')[7].find_all('a')

                for a in actors:
                    actor = a.text
                    actorCd = a.get('href')
                    actorCd = re.findall("\d+", actorCd)[0]
                    actor_ex = {'skp_actor': actor, 'actorCd': actorCd}
                    actor_list.append(actor_ex)

                for f in actor_list:
                    result_ = {'skp_title': title, 'skp_actor': f['skp_actor'], 'actorCd': f['actorCd'],
                               'skp_reservation_url': reservation_url, 'skp_genre': genre, 'skp_date': date,
                               'skp_location': location, 'skp_img_url': img, 'skp_ticketing_url': ticket_url}

                    final_list.append(result_)

            except:
                actor_list = None


        if has_duplicates(final_list):
            break
        else:
            pass

    # 중복 제거
    final_list = list(map(dict, collections.OrderedDict.fromkeys(tuple(sorted(d.items())) for d in final_list)))

    print(f"[INFO] {genre_} crawling DONE !")

    return final_list



def concat() :
    total = final_list_1 + final_list_2 + final_list_3
    total = pd.DataFrame(total, columns=['skp_pk', 'skp_title', 'skp_actor', 'actorCd', 'skp_ticketing_url',
                                         'skp_reservation_url', 'skp_genre', 'skp_date', 'skp_location', 'skp_img_url',
                                         'regist_date', 'update_date'])

    old_df = pd.read_excel(path + 'DB.xlsx', index_col=0)

    total['Checked'] = True
    old_df['Checked'] = False
    concat_df = pd.concat([total, old_df], axis=0)
    unique_df = concat_df[~concat_df.duplicated(subset=['skp_title', 'skp_actor'], keep=False)]
    all_df = concat_df.drop_duplicates(subset=['skp_title', 'skp_actor'], keep='first')
    unique_df_false = unique_df[unique_df['Checked'] == True]
    del unique_df_false['Checked']
    del total['Checked']
    del old_df['Checked']

    all_df.to_excel(path + 'DB.xlsx')

    print(f"[INFO] {len(unique_df_false)} 명이 새로 크롤링 되었습니다. ")
    return unique_df_false


# actorCd로 1차 매핑
def mapping_1(unique_df_false):
    conn, engine = db_connection()

    mapping_list = []

    unique_df_false = unique_df_false.astype({'actorCd': int})
    unique_df_false_drop = unique_df_false

    for i, row in unique_df_false.iterrows():
        skp_actor = row['skp_actor']
        actorCd = row['actorCd']
        skp_title = row['skp_title']

        qry = f"select * from star_ko_performance_connection where actorCd={actorCd}"
        data = pd.read_sql(qry, conn)

        if len(data) == 0:
            pass
        else:
            series_id = data['series_id'][0]
            mapping1_ = {'actorCd': actorCd, 'skp_actor': skp_actor, 'series_id': series_id, 'skp_title': skp_title}
            mapping_list.append(mapping1_)

            idx_num = unique_df_false_drop[unique_df_false_drop['actorCd'] == actorCd].index
            unique_df_false_drop = unique_df_false_drop.drop(idx_num)

    print(mapping_list)
    print(f"[INFO] 1차 자동 매핑 결과 --> {len(mapping_list)} 명 매핑 완료 ")

    conn.close()

    return unique_df_false, unique_df_false_drop, mapping_list    # 원 데이터   # 1차 매핑 후 나머지 데이터   # 1차 매핑된 데이터


def mapping_2(unique_df_false_drop, mapping_list) :
    conn, engine = db_connection()

    mapping2 = []
    newPeople = []
    newPeople2 = []

    mappingCd = []
    newPeopleCd = []
    newPeople2Cd = []


    for j, row in tqdm(unique_df_false_drop.iterrows(), total=unique_df_false_drop.shape[0]):
        skp_actor = row['skp_actor']
        actorCd = row['actorCd']
        skp_title = row['skp_title']
        skp_actor_url = 'http://www.playdb.co.kr/artistdb/detail.asp?ManNo=' + str(actorCd)

        job = ''
        birth = ''
        debut = ''
        agency = ''
        actor_img = ''
        idx = ''

        qry = f"select * from star_ko_data where cd_name ='{skp_actor}' and cd_is_use = 1"
        celeb = pd.read_sql(qry, conn)

        if len(celeb) >= 1:

            html = requests.get(skp_actor_url).text
            soup = BeautifulSoup(html, 'html.parser')

            dt_in_profile = soup.find('div', {'class': 'detaillist2'}).select('dt')
            dd_in_profile = soup.find('div', {'class': 'detaillist2'}).select('dd')

            for row_len in range(len(dt_in_profile)):
                title = dt_in_profile[row_len].text
                inner = dd_in_profile[row_len].text

                if '직업' in title:
                    job = inner

                if '생년' in title:
                    birth = inner
                    birth = birth.replace(".", "-")

                if '데뷔' in title:
                    debut = inner

                if '소속사' in title:
                    agency = inner

            try:
                for c, crow in celeb.iterrows():
                    series_id = crow['series_id']
                    cd_birth = str(crow['cd_birth'])[:10]

                if birth == cd_birth: # 생일만 가지고 매핑.. 플레이디비에 정보가 별로 없음
                    mapping2_ = {'actorCd': actorCd, 'skp_actor': skp_actor, 'series_id': series_id,
                                 'skp_title': skp_title}
                    mapping2.append(mapping2_)
                    raise NotImplementedError

                else:  # 수기매핑

                    #                 actor_img = soup.find('div', {'class':'psdetail_photo'}).select('img')[0]['src']

                    print("---------- 플레이디비 ----------")
                    #                 try:
                    #                     pd_response = requests.get(actor_img)
                    #                     pd_img = Image.open(BytesIO(pd_response.content))
                    #                     display(pd_img)
                    #                 except Exception as e:
                    #                     print(e)

                    print("출연작품: ", skp_title)
                    print("이름: ", skp_actor, "| 직업: ", job, "| 생일: ", birth, "| 데뷔년일: ", debut, "| 소속사: ", agency)
                    print("플레이디비: ", skp_actor_url)

                    for c, crow in celeb.iterrows():
                        cd_name = crow['cd_name']
                        series_id = crow['series_id']
                        cd_category = crow['cd_basic_info']
                        cd_gender = crow['cd_gender']
                        cd_birth = str(crow['cd_birth'])
                        cd_agency = crow['cd_agency']
                        cd_solr_search = crow['cd_solr_search']
                        cd_debut = crow['cd_debut']
                        try:
                            cd_debut = cd_debut[:4]
                        except:
                            pass
                        # cd_img_url = crow['profile_url_main']
                        #
                        # keywords = ['man', 'woman', 'group']
                        # result = any(keyword in cd_img_url for keyword in keywords)

                        print("---------- 셀럽 데이터 ----------")
                        #                     if not result:  # 스타ai에 이미지가 있으면
                        #                         print("seires_id: ", series_id)
                        #                         try:
                        #                             cd_img_url = crow['profile_url_main']
                        #                             cd_response = requests.get(cd_img_url)
                        #                             cd_img = Image.open(BytesIO(cd_response.content))
                        #                             cd_img = cd_img.resize((120, 150))
                        #                             display(cd_img)
                        #                         except:
                        #                             pass

                        #                     else:
                        #                         pass

                        donut = f"http://dev.mycelebs.com/donut/CelebImage/ShowManage/{series_id}"
                        print("series_id: ", series_id)
                        print("이름: ", cd_name, "| 직업: ", cd_category, "| 생일: ", cd_birth[0:-9],
                              "| 데뷔: ", cd_debut, "| 소속사: ", cd_agency)
                        print("도넛: ", donut)

                        idx = ''
                        idx = input("series_id를 매핑해주세요 : ")

                        if idx == '':
                            newPeople_ = {'actorCd': actorCd, 'skp_actor': skp_actor, 'series_id': idx,
                                          'skp_title': skp_title, 'birth':birth}
                            newPeople.append(newPeople_)
                        else:
                            mapping2_ = {'actorCd': actorCd, 'skp_actor': skp_actor, 'series_id': series_id,
                                         'skp_title': skp_title}
                            mapping2.append(mapping2_)
                            raise NotImplementedError
            except:
                pass

        else:
            # newPeople_ = {'actorCd': actorCd, 'skp_actor': skp_actor, 'series_id': idx, 'skp_title': skp_title}
            newPeople_ = {'actorCd': actorCd, 'skp_actor': skp_actor, 'series_id': idx, 'skp_title': skp_title, 'birth': birth}
            newPeople.append(newPeople_)

    print(f"[INFO] 2차 수기 매핑 결과 --> {len(mapping2)} 명 매핑 완료 !")
    print(f"[INFO] 2차 수기 매핑 결과 --> {len(newPeople)} 명 신규 개체 -> 인물등록을 진행하세요. ")

    mapping_list.extend(mapping2)

    # # 중복 제거
    # for i in newPeople:
    #     if i not in mapping_list:
    #         newPeople2.append(i)

    for i in newPeople :
        newPeopleCd.append(i['actorCd'])

    for i in mapping_list :
        mappingCd.append(i['actorCd'])

    for i in newPeopleCd:
        if i not in mappingCd:
            newPeople2Cd.append(i)

    for i in newPeople2Cd :
        for j in range(len(newPeople)) :
            if newPeople[j]['actorCd'] == i :
                newPeople2.append(newPeople[j])

    conn.close()

    return mapping_list, mapping2, newPeople2


def make_star_ko_performance(unique_df_false, mapping_list, mapping2, newPeople2) :

    mapping_all = pd.DataFrame(mapping_list, columns=['skp_title', 'actorCd', 'skp_actor', 'series_id'])
    mapping_all = mapping_all.drop_duplicates()
    mapping_new = pd.DataFrame(mapping2, columns=['skp_title', 'actorCd', 'skp_actor', 'series_id'])
    mapping_new = mapping_new.drop_duplicates()

    # newPeople_df = pd.DataFrame(newPeople2, columns=['skp_title', 'actorCd', 'skp_actor', 'series_id'])
    newPeople_df = pd.DataFrame(newPeople2, columns=['skp_title', 'actorCd', 'skp_actor', 'series_id', 'birth'])
    newPeople_df = newPeople_df.drop_duplicates()

    # movie_people_connection
    mapping_db = mapping_new[['actorCd', 'series_id', 'skp_actor']]
    mapping_db['update_date'] = data_date

    # star_ko_performance & star_ko_performance_actor_name
    db = pd.merge(unique_df_false, mapping_all, how='right', on=['skp_title', 'skp_actor'])
    db_performance = db[
        ['skp_pk', 'series_id', 'skp_title', 'skp_img_url', 'skp_location', 'skp_date', 'skp_reservation_url',
         'regist_date', 'update_date']]
    db_actor_name = db[
        ['skp_pk', 'series_id', 'skp_title', 'skp_actor', 'skp_ticketing_url', 'skp_reservation_url', 'skp_genre',
         'skp_date', 'skp_location', 'skp_img_url', 'regist_date', 'update_date']]
    db_performance = db_performance[~db_performance['series_id'].isnull()]
    db_performance = db_performance[db_performance['series_id'] != '']
    db_performance['regist_date'] = data_date
    db_performance['update_date'] = data_date


    # mapping_df     -> 매핑된 애들 movie_people_connection 에 insert
    # newPeople_df   -> 인물등록할 애들
    # db_performance -> 매핑된 애들 star_ko_performance 에 공연 정보 insert
    # db_actor_name  -> star_ko_performance 에 insert한 배우들 이름 star_ko_performance_actor_name애 insert
    return mapping_db, newPeople_df, db_performance, db_actor_name


# star_ko_performance_actor_name 에 insert하기 위해
# 배우 이름 + 공연 실황(공연 중, 공연 종료, 공연 예정, 오픈런)
def make_performance_actor_name() :
    conn, engine = db_connection()

    qry = 'select * from star_ko_performance_actor_name'
    data = pd.read_sql(qry, conn)

    concat_df = pd.concat([data, db_actor_name], axis=0)
    concat_df = concat_df.reset_index()
    del concat_df['index']

    date = concat_df['skp_date']
    progress = []

    for i in tqdm(range(len(date))):
        d_date = date[i].split('~')
        start = d_date[0]
        start = pd.to_datetime(start)
        last = d_date[1].strip()
        try:
            last = pd.to_datetime(last)
            if start < now < last:
                pro = "공연중"
            elif now > last:
                pro = "공연종료"
            else:
                pro = "공연예정"
        except:
            if last == '오픈런':
                pro = "공연중"
            else:
                pro = "null"
        progress.append(pro)

    concat_df['skp_progress'] = progress
    concat_df = concat_df[
        ['skp_pk', 'series_id', 'skp_title', 'skp_actor', 'skp_ticketing_url', 'skp_reservation_url', 'skp_genre',
         'skp_date', 'skp_location', 'skp_progress', 'skp_img_url', 'regist_date', 'update_date']]
    concat_df['regist_date'] = data_date
    concat_df['update_date'] = data_date

    # concat_df.to_excel("star_ko_performance_actor_name.xlsx")

    print(f"[INFO] performance_actor_name generate DONE ! ")

    return concat_df


def db_update(mapping_db, db_performance) :
    conn, engine = db_connection()

    mapping_db.to_sql('star_ko_performance_connection', engine, if_exists='append', index=None)
    print(f"[INFO] star_ko_performance_connection db insert 완료 ")

    db_performance.to_sql('star_ko_performance', engine, if_exists='append', index=None)
    print(f"[INFO] star_ko_performance db insert 완료 ")

    conn.close()
    engine.dispose()


def db_trunc_insert(concat_df) :
    host_url = "db.ds.mycelebs.com"
    user_nm = 'j_eungg'
    passwd = "wldms10529"
    port_num = 3306
    db_name = 'star_ko'
    engine = create_engine(f'mysql+pymysql://{user_nm}:{passwd}@{host_url}:{port_num}/{db_name}?charset=utf8mb4')

    my = engine.connect()

    sql = "TRUNCATE TABLE star_ko_performance_actor_name"
    my.execute(sql)
    concat_df.to_sql('star_ko_performance_actor_name', my, if_exists='append', index=None)
    print(f"[INFO] star_ko_performance_actor_name db truncate & insert 완료 ")

    my.close()


def donut_update() :
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--headless')
    driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)

    pid = 'mycelebsTempUser'
    ppw = 'mycelebs!@rookie'
    driver.get(f'http://{pid}:{ppw}@dev.mycelebs.com/donut/')
    # alert = driver.switch_to_alert()
    alert = driver.switch_to.alert
    alert.accept()
    time.sleep(2.0)

    driver.find_element_by_css_selector('#adminId').send_keys('rookie')
    driver.find_element_by_css_selector('#adminPw').send_keys('1234')
    btn = driver.find_element_by_css_selector('#loginForm > button')
    btn.click()

    for i, row in mapping_db.iterrows():
        actorCd = row['actorCd']
        series_id = row['series_id']
        skp_actor = row['skp_actor']

        donut = 'http://dev.mycelebs.com/donut/Celeb/ShowManageCeleb/' + str(series_id)
        driver.get(donut)

        driver.find_element_by_xpath('//*[@id="s_playdb_people_id"]').clear()
        driver.find_element_by_xpath('//*[@id="s_playdb_people_id"]').send_keys(str(actorCd))

        # 등록 버튼!
        driver.find_element_by_xpath('//*[@id="write"]/div/div/div[2]/div/input').click()

        # 정상처리 되었습니다.
        time.sleep(1)
        alert.accept()

        print(skp_actor, "-", series_id, '도넛 업데이트 완료')


def report() :
    list_ = db_performance.series_id.tolist()
    list_ = list(map(str, list_))

    print('*{0}월 {1}일 스타 공연 업데이트 요청드립니다. '.format(t_month, t_day))
    print('- DB명 : star_ko > star_ko_performance')
    print('- 추가 스타정보 : {}명'.format(len(list_)))
    print(" ".join(list_), end='\n\n')



# 인물등록해야하는 개체들
def produce_dataframe() :

    profile_playdb = []

    for i in newPeople_df['actorCd'] :
        profile_url = 'http://www.playdb.co.kr/artistdb/detail.asp?ManNo='+str(i)
        profile_playdb.append(profile_url)

    newPeople_df['actor_url'] = profile_playdb

    empty_frame = pd.DataFrame(
        columns=("actorCd", "actor_url", "날짜", "요청자", "카테고리", "이름", "daum", "naver", "cd_idx", "생일"))

    empty_frame["actorCd"] = list(newPeople_df.actorCd)
    empty_frame["actor_url"] = list(newPeople_df.actor_url)
    empty_frame["날짜"] = date__
    empty_frame["요청자"] = "이지은"
    empty_frame["카테고리"] = "공연"
    empty_frame["이름"] = list(newPeople_df.skp_actor)
    empty_frame["daum"] = "x"
    empty_frame["naver"] = ""
    empty_frame["cd_idx"] = ""
    empty_frame["생일"] = list(newPeople_df.birth)

    return empty_frame


# 같은 이름 다른 인물 모듈 스크롤 하기
# 특정 시간동안 계속해서 scroll down
def doScrollDown(whileSeconds, naver_drv):
    start = datetime.datetime.now()
    end = start + datetime.timedelta(seconds=whileSeconds)

    while True:
        element = naver_drv.find_element_by_xpath('//*[@id="main_pack"]/section[2]/div/div[1]/div/div[2]')
        naver_drv.execute_script("arguments[0].scrollBy(0, 3000)", element)
        time.sleep(1)
        if datetime.datetime.now() > end:
            break


def get_naver_url(empty_frame):
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--headless')
    naver_drv = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)

    profile_naver = []

    for i, row in tqdm(empty_frame.iterrows(), total=empty_frame.shape[0]):
        name = row['이름']
        actor_url = row['actor_url']
        naver_url = row['naver']
        print(name)

        people_url = []

        # 플레이디비에서 작품 크롤링
        url_playdb = actor_url
        html_plydb = requests.get(url_playdb).text
        soup_playdb = BeautifulSoup(html_plydb, 'html.parser')

        ptitle_list = []
        ptitle = soup_playdb.find_all('td', {'class': 'ptitle'})

        for pt in range(len(ptitle)):
            ptitles = ptitle[pt].text
            ptitle_list.append(ptitles)

        # 네이버에서 동명이인 리스트 크롤링
        naver_drv.get(f'https://search.naver.com/search.naver?where=nexearch&sm=top_hty&fbm=1&ie=utf8&query={name}')

        naver_source = naver_drv.page_source
        naver_soup = BeautifulSoup(naver_source, "html.parser")

        try:  # NotImplementedError로 빠져나오기 위함

            # 세가지 케이스 존재

            # 1. 메인에 한명 노출, 하단에 동명이인 리스트
            # 1.1 메인 먼저 크롤링
            if naver_soup.select("div[class='cm_top_wrap _sticky _custom_select']"):
                ntitle_list = []
                ntitle = naver_soup.find_all('ul', {'class': 'list'})[0].find_all('a', {'class': '_text'})

                for nt in range(len(ntitle)):
                    ntitles = ntitle[nt].text
                    ntitle_list.append(ntitles)

                for title in ntitle_list:
                    if title in ptitle_list:
                        naver_drv.find_elements_by_xpath(
                            '//*[@id="main_pack"]/section[1]/div[1]/div[3]/div/div/ul/li[1]/a/span')[
                            0].click()  # 한번 클릭 for os값
                        profile_naver.append({'이름': name, 'url': naver_drv.current_url})
                        print("[INFO] {} - 동일 작품이 발견되었습니다.".format(title))
                        raise NotImplementedError

                if '같은 이름 다른 인물' in naver_soup.text:
                    try:
                        naver_drv.find_element_by_partial_link_text('동명이인').click()  # 동명이인 더보기
                        doScrollDown(10)

                        naver_source = naver_drv.page_source
                        naver_soup = BeautifulSoup(naver_source, "html.parser")

                        people = naver_soup.find_all('ul', {'class': '_list'})
                        # print('메인 - 동명이인 - 전체보기')

                    except:
                        people = naver_soup.select("div[class='cm_info_box scroll_img_vertical_87_98']")[0].select(
                            "li[class=_content]")
                        # print('메인 - 동명이인 - 스크롤')

                    for p in range(len(people)):
                        naver_base = 'https://search.naver.com/search.naver'
                        try:
                            people_url_ = naver_base + people[p].get('href')
                        except:
                            people_url_ = naver_base + people[p].select('a')[0].get('href')
                        people_url.append(people_url_)

                    # 동명이인 작품 크롤링
                    for pu in people_url:
                        html_naver = requests.get(pu).text
                        soup_naver = BeautifulSoup(html_naver, 'html.parser')

                        time.sleep(random.uniform(0.1, 0.5))

                        ntitle_list = []
                        ntitle = soup_naver.find_all('ul', {'class': 'list'})[0].find_all('a', {'class': '_text'})

                        for nt in range(len(ntitle)):
                            ntitles = ntitle[nt].text
                            ntitle_list.append(ntitles)

                        for title in ntitle_list:
                            if title in ptitle_list:
                                profile_naver.append({'이름': name, 'url': pu})
                                print("[INFO] {} - 동일 작품이 발견되었습니다.".format(title))
                                raise NotImplementedError

                else:
                    raise NotImplementedError
                    # print('메인 - 동명이인 없음')


            # 2. 세로 리스트
            elif naver_soup.select('div[class=answer_more]'):
                answer_more = naver_soup.select('div[class=answer_more]')
                for am in range(len(answer_more)):
                    naver_base = 'https://search.naver.com/search.naver'
                    people_url_ = naver_base + answer_more[am].select('a')[0].get('href')
                    people_url.append(people_url_)

                # more_wrap ex. 현석준
                try:
                    more_wrap = naver_soup.select('div[class=more_wrap]')
                    for mw in range(len(more_wrap)):
                        people_url_ = more_wrap[mw].select('a')[0].get('href')
                        people_url.append(people_url_)
                except:
                    pass

                if '같은 이름 다른 인물' in naver_soup.text:
                    try:
                        naver_drv.find_element_by_partial_link_text('동명이인').click()  # 동명이인 더보기
                        doScrollDown(10)

                        naver_source = naver_drv.page_source
                        naver_soup = BeautifulSoup(naver_source, "html.parser")

                        people = naver_soup.find_all('ul', {'class': '_list'})
                        # print('메인 - 동명이인 - 전체보기')

                    except:
                        people = naver_soup.select("div[class='cm_info_box scroll_img_vertical_87_98']")[0].select(
                            "li[class=_content]")
                        # print('메인 - 동명이인 - 스크롤')

                    for p in range(len(people)):
                        naver_base = 'https://search.naver.com/search.naver'
                        try:
                            people_url_ = naver_base + people[p].get('href')
                        except:
                            people_url_ = naver_base + people[p].select('a')[0].get('href')
                        people_url.append(people_url_)

                # 동명이인 작품 크롤링
                for pu in people_url:
                    html_naver = requests.get(pu).text
                    soup_naver = BeautifulSoup(html_naver, 'html.parser')

                    time.sleep(random.uniform(0.1, 0.5))

                    ntitle_list = []
                    try:
                        ntitle = soup_naver.find_all('ul', {'class': 'list'})[0].find_all('a', {'class': '_text'})

                        for nt in range(len(ntitle)):
                            ntitles = ntitle[nt].text
                            ntitle_list.append(ntitles)

                        for title in ntitle_list:
                            if title in ptitle_list:
                                profile_naver.append({'이름': name, 'url': pu})
                                print("[INFO] {} - 동일 작품이 발견되었습니다.".format(title))
                                raise NotImplementedError
                    except:
                        pass


            # 4. 가로 리스트 / 프로필 없음
            else:
                if '같은 이름 다른 인물' in naver_soup.text:
                    try:
                        naver_drv.find_element_by_partial_link_text('동명이인').click()  # 동명이인 더보기
                        doScrollDown(10)

                        naver_source = naver_drv.page_source
                        naver_soup = BeautifulSoup(naver_source, "html.parser")

                        people = naver_soup.find_all('ul', {'class': '_list'})
                        # print('가로 - 동명이인 - 전체보기')

                    except:
                        people = naver_soup.select("div[class='cm_info_box scroll_img_vertical_87_98']")[0].select(
                            "li[class=_content]")
                        # print('가로 - 동명이인 - 스크롤')

                    for p in range(len(people)):
                        naver_base = 'https://search.naver.com/search.naver'
                        try:
                            people_url_ = naver_base + people[p].get('href')
                        except:
                            people_url_ = naver_base + people[p].select('a')[0].get('href')
                        people_url.append(people_url_)

                    # 동명이인 작품 크롤링
                    for pu in people_url:
                        html_naver = requests.get(pu).text
                        soup_naver = BeautifulSoup(html_naver, 'html.parser')

                        time.sleep(random.uniform(0.1, 0.5))

                        ntitle_list = []
                        try:
                            ntitle = soup_naver.find_all('ul', {'class': 'list'})[0].find_all('a', {'class': '_text'})

                            for nt in range(len(ntitle)):
                                ntitles = ntitle[nt].text
                                ntitle_list.append(ntitles)

                            for title in ntitle_list:
                                if title in ptitle_list:
                                    profile_naver.append({'이름': name, 'url': pu})
                                    print("[INFO] {} - 동일 작품이 발견되었습니다.".format(title))
                                    raise NotImplementedError
                        except:
                            pass

                else:
                    print("프로필 없음")
                    raise NotImplementedError


        except:
            pass

    return profile_naver


def produce_dataframe2(empty_frame, profile_naver):
    for pn in profile_naver:
        for key, value in pn.items():
            for e in range(len(empty_frame)):
                if empty_frame['이름'][e] == value:
                    empty_frame['naver'][e] = pn['url']

    # idx = empty_frame[empty_frame['naver'] == ''].index
    # empty_frame_drop = empty_frame.drop(idx)

    return empty_frame


def db_update2(after) :
    ans = input('인물등록이 완료되었나요?(y/n)')
    if ans.lower() == 'y' :

        conn, engine = db_connection()

        after = after[['actorCd', 'cd_idx', '이름']]
        after = after.rename({'actorCd': 'actorCd', 'cd_idx': 'series_id', '이름': 'skp_actor'}, axis=1)
        after = after.astype({'series_id': 'int'})
        after['update_date'] = data_date
        after.to_sql('star_ko_performance_connection', engine, if_exists='append', index=None)
        print("[INFO] 인물등록 결과 -> star_ko_performance_connection db insert 완료")

        conn.close()
        engine.dispose()

    else :
        pass


if __name__ == '__main__':

    now = datetime.datetime.now()
    date__ = now.strftime('%Y-%m-%d')
    data_date = now.strftime('%Y-%m-%d %H:%M:00')

    y = now - timedelta(1)
    h = now - timedelta(3)
    holiyday = h.strftime('%Y%m%d')
    yesterday = y.strftime('%Y%m%d')

    t_month = now.strftime('%m')
    t_day = now.strftime('%d')

    path = '/Users/jieun/Desktop/업무/인물매핑/playDB/'


    """ 1. 크롤링 """
    final_list_1 = crawling(craw_genre="뮤지컬")
    final_list_2 = crawling(craw_genre="연극")
    final_list_3 = crawling(craw_genre="콘서트")

    unique_df_false = concat()

    """ 2.1 connection으로 1차 매핑 """
    unique_df_false, unique_df_false_drop, mapping_list = mapping_1(unique_df_false)
    """ 2.2 나머지는 수기로 2차 매핑 """
    mapping_list, mapping2, newPeople2 = mapping_2(unique_df_false_drop, mapping_list)

    """ 3. 데이터 정제 및 DB 업로드 """
    mapping_db, newPeople_df, db_performance, db_actor_name = make_star_ko_performance(unique_df_false, mapping_list, mapping2, newPeople2)
    concat_df = make_performance_actor_name()
    concat_df.to_excel(path + "star_ko_performance_actor_name.xlsx")

    db_update(mapping_db, db_performance)
    db_trunc_insert(concat_df) # 현재 시간에 따라 공연이 진행 중인지, 종료되었는지 달라지므로 truncate 후 insert

    donut_update()

    """ 4. 보고 """
    report()

    """ 5. 신규 인물 추출 """
    empty_frame = produce_dataframe()
    profile_naver = get_naver_url(empty_frame)
    empty_frame = produce_dataframe2(empty_frame, profile_naver)
    empty_frame.to_excel(path + '인물등록/' + date__ + '_공연_인물등록.xlsx')

    ###################################################################
    """ 6. 신규 인물 db insert """
    # 인물등록 후, 엑셀 파일에 cd_idx 붙여넣고 load
    after = pd.read_excel(path + '인물등록/' + date__ + '_공연_인물등록.xlsx', index_col=0)
    # after = pd.read_excel('/Users/jieun/Desktop/업무/인물매핑/playDB/인물등록/2021-08-02_공연_인물등록.xlsx', index_col=0)
    db_update2(after)
