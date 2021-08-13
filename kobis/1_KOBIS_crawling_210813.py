

'''
목표 : 코비스(https://www.kobis.or.kr/) 내 영화 인물들(배우, 감독)과 마셀 데이터 내 인물들을 매핑 ! 하기 위해 생일 정보 크롤링
크롤링 대상 : 코비스에 등록된 모든 배우, 감독, 조감독

-> 인물 매핑을 위해선 인물들의 각종 정보가 필요함
-> 따라서 코비스 인물들의 생일 정보, 국적 정보를 추가로! 수집함
    db명 : kobis_cast_birth_country(배우), kobis_crew_birth_country(감독)
    ( 생일, 국적을 굳이 따로 수집하는 이유는.. 생일이랑 국적은 api에서 안내려옴ㅠ )
-> 적당한 페이지 수를 입력하여 주기적으로 수집해야함(오래 걸리니까 돌려놓고 퇴근하자ㅎㅅㅎ!)
-> 수집 후 최종 db 테이블에 merge
    db명 : kobis_people_list

'''



import pandas as pd
from selenium import webdriver
from bs4 import BeautifulSoup
from tqdm import tqdm
import time
from webdriver_manager.chrome import ChromeDriverManager
import re
import pymysql


# db connect
def db_connection(host_name='ds'):
    host_url = "db.ds.mycelebs.com"
    user_nm = "j_eungg"
    passwd = "mycelebs"
    port_num = 3306
    db_name = "maimovie_kr"
    conn = pymysql.connect(host=host_url, user=user_nm, passwd=passwd, port = port_num, charset='utf8',
                           db = db_name,cursorclass=pymysql.cursors.DictCursor)
    # cursorclass=pymysql.cursors.DictCursor 추가 -> 데이터프레임으로 다루기 쉽게 딕셔너리 형태로 결과 반환해줌, cursor는 튜플형태
    # db=db,
    return conn


# get driver
def generate_driver(who):
    chrome_options = webdriver.ChromeOptions()
    # chrome_options.add_argument('headless')
    driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)
    driver.get(f'https://www.kobis.or.kr/kobis/business/mast/peop/searchPeopleList.do')

    driver.find_element_by_xpath('//*[@id="content"]/div[3]/div[2]/a[1]').click()  # 더보기
    driver.find_element_by_xpath('//*[@id="sRoleStr"]').click()  # 분야별 클릭
    time.sleep(3)

    if who == '배우':
        driver.find_element_by_xpath('//*[@id="mul_chk_det20"]').click()  # 배우 체크박스
        time.sleep(1)

    elif who == '감독':
        driver.find_element_by_xpath('//*[@id="mul_chk_det13"]').click()  # 감독 체크박스
        driver.find_element_by_xpath('//*[@id="mul_chk_det18"]').click()  # 조감독 체크박스
        time.sleep(1)

    else :
        print("직업 입력 에러!")


    driver.find_element_by_xpath('//*[@id="layerConfirmChk"]').click()  # 확인
    driver.find_element_by_xpath('//*[@id="searchForm"]/div[1]/div[3]/button[1]').click()  # 조회
    time.sleep(2)
    return driver


# crawling & db insert
def additional_crawling(page_num):

    conn = db_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    print("[INFO] crawling start ...")

    driver = generate_driver(who=people)
    pages = range(1, int(page_num) + 1)

    for p in tqdm(pages):

        source = driver.page_source
        soup = BeautifulSoup(source, "lxml")

        # 마지막 페이지는 인물 리스트가 10명이 아닐 가능성 있음
        # 리스트 몇명인지 먼저 구하고 range()에 넣기
        getRange = soup.select('#content > div.rst_sch > table > tbody')
        range_ = str(getRange[0]).count('<tr>')

        # 크롤링 시작
        for n in range(1, range_ + 1):
            peopleNm = soup.select(f'#content > div.rst_sch > table > tbody > tr:nth-child({n}) > td:nth-child(1) > span > a')[0].text  # 영화인명
            peopleNm = re.sub("\!|\'|\?", "", peopleNm)
            peopleCd = soup.select(f'#content > div.rst_sch > table > tbody > tr:nth-child({n}) > td:nth-child(3)')[0].text  # 영화인코드
            birth = soup.select(f'#content > div.rst_sch > table > tbody > tr:nth-child({n}) > td:nth-child(6)')[0].text  # 생년월일
            #         비어있는 생년월일 Null 값 처리
            #         if birth == '' :
            #             birth =  None
            #         else :
            #             pass

            country = soup.select(f'#content > div.rst_sch > table > tbody > tr:nth-child({n}) > td:nth-child(7) > span')[0].text  # 국적

            # db insert
            if people == '배우':
                query = f'INSERT INTO `kobis_cast_birth_country` (`peopleCd`, `peopleNm`, `birth`, `country`, `regist_date`, `update_date`) VALUES (\'{peopleCd}\', \'{peopleNm}\', \'{birth}\', \'{country}\', NOW(), NOW());'
                cursor.execute(query)
                conn.commit()

            elif people == '감독':
                query = f'INSERT INTO `kobis_crew_birth_country` (`peopleCd`, `peopleNm`, `birth`, `country`, `regist_date`, `update_date`) VALUES (\'{peopleCd}\', \'{peopleNm}\', \'{birth}\', \'{country}\', NOW(), NOW());'
                cursor.execute(query)
                conn.commit()

        print("    ", p, "페이지 크롤링 완료")

        # 페이지 끝자리 수 구하기
        p_ = str(p)
        p_split = p_[-1]

        # 페이지 끝자리가 0이면, 즉 10의 배수이면 다음 버튼 누르고,
        if p_split == '0':
            driver.find_element_by_xpath('//*[@id="pagingForm"]/div/a[3]/span').click()
            time.sleep(3)

        # 끝자리가 0이 아니면 p+1 버튼 누르기
        else:
            driver.find_element_by_xpath(f'//*[@id="pagingForm"]/div/ul/li[{int(p_split) + 1}]/a').click()  # 페이지 넘기기
            time.sleep(3)

    print("[INFO] crawling DONE!")

    driver.close()
    conn.close()


# 중복 제거
def delete_duplicate(who):

    conn = db_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    print("[INFO] 중복 row 제거 시작 ...")

    if who == '배우':
        query = "SELECT COUNT(*) from kobis_cast_birth_country"
        before_cnt = pd.read_sql(query, con=conn)
        before_cnt = before_cnt.iloc[0, 0]

        query = f"DELETE b FROM kobis_cast_birth_country a, kobis_cast_birth_country b WHERE a.pk > b.pk AND a.peopleCd = b.peopleCd;"
        cursor.execute(query)
        conn.commit()

        query = "SELECT COUNT(*) from kobis_cast_birth_country"
        after_cnt = pd.read_sql(query, con=conn)
        after_cnt = after_cnt.iloc[0, 0]

        print("[INFO] kobis_cast_birth_country에서 ", before_cnt - after_cnt, "개의 중복 row가 삭제되었습니다.")


    elif who == '감독':
        query = "SELECT COUNT(*) from kobis_crew_birth_country"
        before_cnt = pd.read_sql(query, con=conn)
        before_cnt = before_cnt.iloc[0, 0]

        query = f"DELETE b FROM kobis_crew_birth_country a, kobis_crew_birth_country b WHERE a.pk > b.pk AND a.peopleCd = b.peopleCd;"
        cursor.execute(query)
        conn.commit()

        query = "SELECT COUNT(*) from kobis_crew_birth_country"
        after_cnt = pd.read_sql(query, con=conn)
        after_cnt = after_cnt.iloc[0, 0]

        print("[INFO] kobis_crew_birth_country에서 ", before_cnt - after_cnt, "개의 중복 row가 삭제되었습니다.")

    conn.close()


# 머지 진행
def db_execute_query(who):

    conn = db_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    print("[INFO] kobis_people_list에 merge 진행 중 ...")

    if who == '배우':
        qry1 = """update kobis_people_list    
                     set kobis_people_list.birth = (select birth 
                                                      from kobis_cast_birth_country 
                                                     where kobis_cast_birth_country.peopleCd = kobis_people_list.peopleCd);"""
        cursor.execute(qry1)
        conn.commit()

        qry2 = """update kobis_people_list    
                     set kobis_people_list.country = (select country 
                                                      from kobis_cast_birth_country 
                                                     where kobis_cast_birth_country.peopleCd = kobis_people_list.peopleCd);"""
        cursor.execute(qry2)
        conn.commit()

    elif who == '감독':
        qry3 = """update kobis_people_list    
                     set kobis_people_list.birth = (select birth 
                                                      from kobis_crew_birth_country 
                                                     where kobis_crew_birth_country.peopleCd = kobis_people_list.peopleCd);"""
        cursor.execute(qry3)
        conn.commit()

        qry4 = """update kobis_people_list    
                     set kobis_people_list.country = (select country 
                                                      from kobis_crew_birth_country 
                                                     where kobis_crew_birth_country.peopleCd = kobis_people_list.peopleCd);"""
        cursor.execute(qry4)
        conn.commit()

    conn.close()

    print("DONE !!!! ")




if __name__ == '__main__':

    people = input("감독 vs 배우: ")
    number = input("페이지번호를 입력하세요: ")

    driver = generate_driver(who=people)
    additional_crawling(page_num=number)
    delete_duplicate(who=people)
    db_execute_query(who=people)



