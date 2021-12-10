'''
(21/11/22)
1. star_album_genre2 변동 내역도 도넛에 반영

'''

import pandas as pd
from selenium import webdriver
from tqdm import tqdm
import time
from webdriver_manager.chrome import ChromeDriverManager
import pymysql
from sqlalchemy import create_engine

def db_connection(host_name='ds'):
    host_url = "db.ds.mycelebs.com"
    user_nm =
    passwd =
    port_num = 3306
    db_name = "star_ko"
    conn = pymysql.connect(host=host_url, user=user_nm, passwd=passwd, port=port_num, charset='utf8',
                           db=db_name, cursorclass=pymysql.cursors.DictCursor)
    # cursorclass=pymysql.cursors.DictCursor 추가 -> 데이터프레임으로 다루기 쉽게 딕셔너리 형태로 결과 반환해줌, cursor는 튜플형태
    # db=db,
    engine = create_engine(f'mysql+pymysql://{user_nm}:{passwd}@{host_url}:{port_num}/{db_name}?charset=utf8mb4')
    return conn, engine


def getData() :
    conn, engine = db_connection()

    # star_ko_profile_url
    qry1 = f'SELECT * FROM star_ko_profile_url WHERE update_date >= CURDATE()'
    profile_df = pd.read_sql(qry1, conn)

    # star_album_genre2
    qry2 = f'SELECT * FROM star_album_genre2 WHERE update_date >= CURDATE()'
    vibe_df = pd.read_sql(qry2, conn)

    conn.close()

    return profile_df, vibe_df

# def getData() :
#     conn, engine = db_connection()
#
#     # star_ko_profile_url
#     qry1 = f'SELECT * FROM star_ko_profile_url WHERE update_date >= CURDATE()'
#     profile_df = pd.read_sql(qry1, conn)
#
#     conn.close()
#
#     return profile_df


def donut_update() :

    total = len(profile_df) + len(vibe_df)
    # total = len(profile_df)

    if total == 0 :
        print(f"업데이트할 개체가 없습니다. ")

    else :

        print(f"{len(profile_df) + len(vibe_df)} 개 개체 도넛 업데이트 시작 .. ")
        # print(f"{len(profile_df)} 개 개체 도넛 업데이트 시작 .. ")

        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--headless')
        driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)

        pid =
        ppw =
        driver.get(f'http://{pid}:{ppw}@dev.mycelebs.com/donut/')
        # alert = driver.switch_to_alert()
        alert = driver.switch_to.alert
        alert.accept()
        time.sleep(2.0)

        driver.find_element_by_css_selector('#adminId').send_keys('')
        driver.find_element_by_css_selector('#adminPw').send_keys('')
        btn = driver.find_element_by_css_selector('#loginForm > button')
        btn.click()

        for i, row in tqdm(profile_df.iterrows(), total=profile_df.shape[0]):
            series_id = row['series_id']
            name = row['name']
            daum_people_id = row['daum_people_id']
            try:
                naver_people_id = str(int(row['naver_people_id']))
            except:
                naver_people_id = None
            print(series_id, " - ", name)

            donut = 'http://dev.mycelebs.com/donut/Celeb/ShowManageCeleb/' + str(series_id)
            driver.get(donut)

            # 네이버
            before = driver.find_element_by_xpath('//*[@id="s_naver_people_id"]').get_attribute('value')
            if pd.notnull(naver_people_id):
                if before != naver_people_id:
                    driver.find_element_by_xpath('//*[@id="s_naver_people_id"]').clear()
                    driver.find_element_by_xpath('//*[@id="s_naver_people_id"]').send_keys(naver_people_id)
                    print("[INFO] 네이버 - 수정되었습니다. {0} -> {1}".format(before, naver_people_id))
            else:
                driver.find_element_by_xpath('//*[@id="s_naver_people_id"]').clear()
                print("[INFO] 네이버 - 수정되었습니다. {0} -> {1}".format(before, naver_people_id))

            # 다음
            before = driver.find_element_by_xpath('//*[@id="s_daum_people_id"]').get_attribute('value')
            if pd.notnull(daum_people_id):
                if before != daum_people_id:
                    driver.find_element_by_xpath('//*[@id="s_daum_people_id"]').clear()
                    driver.find_element_by_xpath('//*[@id="s_daum_people_id"]').send_keys(daum_people_id)
                    print("[INFO] 다음 - 수정되었습니다. {0} -> {1}".format(before, daum_people_id))
            else:
                driver.find_element_by_xpath('//*[@id="s_daum_people_id"]').clear()
                print("[INFO] 다음 - 수정되었습니다. {0} -> {1}".format(before, daum_people_id))

            # 등록 버튼!
            driver.find_element_by_xpath('//*[@id="write"]/div/div/div[2]/div/input').click()

            # 정상처리 되었습니다.
            time.sleep(1)
            alert.accept()


        for i, row in tqdm(vibe_df.iterrows(), total=vibe_df.shape[0]):
            series_id = row['series_id']
            name = row['name']
            vibeCd = row['vibeCd']

            print(series_id, " - ", name)

            donut = 'http://dev.mycelebs.com/donut/Celeb/ShowManageCeleb/' + str(series_id)
            driver.get(donut)

            # 바이브
            if vibeCd == -1 :
                driver.find_element_by_xpath('//*[@id="s_vibe_people_id"]').clear()
            else :
                driver.find_element_by_xpath('//*[@id="s_vibe_people_id"]').clear()
                driver.find_element_by_xpath('//*[@id="s_vibe_people_id"]').send_keys(str(vibeCd))
                print("[INFO] 바이브 - 수정되었습니다. ")

            # 등록 버튼!
            driver.find_element_by_xpath('//*[@id="write"]/div/div/div[2]/div/input').click()

            # 정상처리 되었습니다.
            time.sleep(1)
            alert.accept()

        driver.close()


if __name__ == '__main__':
    profile_df, vibe_df = getData()
    # profile_df = getData()
    donut_update()


