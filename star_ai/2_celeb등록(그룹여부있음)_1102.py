'''
(21/10/21)
1. star_ko_data에 naver_people_id, naver_movie_people_id, daum_people_id, adic_people_id, playdb_people_id, vibe_people_id 컬럼 추가됨
  -> 인물등록 과정에서 각 고유값들도 도넛에 인서트하도록 수정

(21/10/22)
1. 기존엔 <매칭 단어 = 이름> 이었으나, 정책이 바뀜
  -> "특정이 가능"한 이름으로 넣어줘야 한다! 예를 들어 뷔의 경우, 기존엔 (매칭 단어 = 뷔) 였다면 지금은 (매칭 단어 = 방탄소년단 뷔, BTS 뷔, 가수 뷔 ..)
  -> 따라서 매칭 단어를 따로 인서트하도록 수정

(21/10/25)
1. 숫자의 경우 string으로 바꾸고 넣었더니 db에 0으로 들어가서, if notnull 할 경우에만 데이터를 넣도록 수정
정'''


from selenium import webdriver
from tqdm import tqdm
import requests
import time
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import pymysql
import random
import numpy as np

# db connect 함수
def db_connection(host_name='ds'):
    host_url = "db.ds.mycelebs.com"
    user_nm = "j_eungg"
    passwd = "mycelebs"
    port_num = 3306
    db_name = "star_ko"
    conn = pymysql.connect(host=host_url, user=user_nm, passwd=passwd, port = port_num, charset='utf8',
                           db = db_name,cursorclass=pymysql.cursors.DictCursor)
    # cursorclass=pymysql.cursors.DictCursor 추가 -> 데이터프레임으로 다루기 쉽게 딕셔너리 형태로 결과 반환해줌, cursor는 튜플형태
    # db=db,
    return conn


# 엑셀에 입력해야 할 정보
# 이름,생일,출신지,키,몸무게,소속팀,포지션,학력,데뷔,그룹/팀여부,수상,경력,공식사이트,인스타그램,페이스북,트위터,기타사이트


star_excel = pd.read_excel('/Users/jieun/Desktop/업무/인물등록/익디 등록.xlsx')

name = star_excel['이름']
cate = star_excel['기본 분류'] 
sex = star_excel['성별']
search_word = star_excel['검색어']
match_word = star_excel['매칭 단어']
search_engine = star_excel['내부 검색식']
real_name = star_excel['본명/영어이름']
job = star_excel['직업']
height = star_excel['키']
weight = star_excel['몸무게']
bmi = star_excel['체질량지수']
bmi_range = star_excel['BMI 범위']
sun_birth = star_excel['양력생일']
moon_birth = star_excel['음력생일']
star = star_excel['별자리']
animals = star_excel['띠']
blood_type = star_excel['혈액형']

birth_site = star_excel['출생지']
sosogsa = star_excel['소속사']
sosog = star_excel['소속']
sosog_team = star_excel['소속팀']
group = star_excel['그룹']
family = star_excel['가족']
debut = star_excel['데뷔']
group_flag = star_excel['그룹/팀 여부']
member = star_excel['멤버']
student = star_excel['학력']
career = star_excel['경력']
award = star_excel['수상']
death = star_excel['사망 여부']

naver_people_id = star_excel['네이버 인물 id']
daum_people_id = star_excel['다음 인물 id']
naver_movie_people_id = star_excel['네이버 영화 인물 id']
adic_people_id = star_excel['광고정보센터 인물 id']
playdb_people_id = star_excel['플레이디비 인물 id']
vibe_people_id = star_excel['바이브 인물 id']

site_1 = star_excel['공식사이트 [Official site]']
site_2 = star_excel['팬카페 [Fan Cafe]']
site_3 = star_excel['블로그 [Blog]']
site_4 = star_excel['미니홈피 [MiniHomepage]']
site_5 = star_excel['트위터 [Twitter]']
site_6 = star_excel['페이스북 [Facebook]']
site_7 = star_excel['인스타그램 [Instagram]']
site_8 = star_excel['유튜브 [Youtube]']
site_9 = star_excel['웨이보 [Weibo]']
site_10 = star_excel['브이라이브 [V LIVE]']



#idx_list = []
error_list = []

#스타 등록하기
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless')
driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)


r = requests.Session()
bQueryError = False
print("솔라쿼리 검수 시작 ...")
# for i in tqdm(range(len(search_engine))):
#     response = r.get(f'http://ba_search_replica.mycelebs.com:9100/solr-4.8.1/ba_crawl/select?q={search_engine[i]}&wt=json&indent=true')
#     if response.status_code != 200:
#         print(str(i+2) + str(search_engine[i]))
#         bQueryError = True
#     time.sleep(1.0)
print("솔라쿼리 검수 완료 !")

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

# print(bQueryError)
if bQueryError is False:
    for i in tqdm(range(len(name))):
        #메인페이지부터 시작할것. - 셀럽 관리 화면까지
        driver.find_element_by_xpath('/html/body/div[1]/div/div[2]/ul/li[1]/a').click()
        driver.find_element_by_xpath('/html/body/div[1]/div/div[2]/ul/li[1]/ul/li[2]/a').click()

        # celeb_data 추가 화면 시작!
        driver.find_element_by_xpath('/html/body/div[2]/div/div/div[1]/a[1]').click()
        time.sleep(1.8)


        # 이름
        driver.find_element_by_xpath('//*[@id="s_cd_name"]').clear()
        driver.find_element_by_xpath('//*[@id="s_cd_name"]').send_keys(name[i])
        # 분류
        # 배우, 모델, 가수, 방송인, 개그맨, 운동선수
        driver.find_element_by_xpath('//*[@id="s_cd_category"]').clear()
        driver.find_element_by_xpath('//*[@id="s_cd_category"]').send_keys(cate[i])
        # 성별

        if sex[i] == '남자' or sex[i] == '남성':
            num_sex = 2
        elif sex[i] == '여자' or sex[i] == '여성':
            num_sex = 3
        elif sex[i] == '남자,여자' or sex[i] == '혼성':
            num_sex = 4
        else:
            num_sex = 1
            error_list.append(name[i] + '성별 조회 안됨')
        driver.find_element_by_xpath('//*[@id="s_cd_gender"]').click()
        driver.find_element_by_xpath('//*[@id="s_cd_gender"]/option[' + str(num_sex) + ']').click()
        # 검색어
        driver.find_element_by_xpath('//*[@id="s_cd_search_word"]').clear()
        driver.find_element_by_xpath('//*[@id="s_cd_search_word"]').send_keys(search_word[i])
        
        # 내부 검색식

        ###################### 에러 발생지점, 매칭 단어 없다고 나올 경우 이 부분 스킵할 것. #########################################
        # 사이트의 결함이며, 코드 자체에는 이상 없음.

        driver.find_element_by_xpath('//*[@id="s_cd_solr_search"]').clear()
        driver.find_element_by_xpath('//*[@id="s_cd_solr_search"]').send_keys(search_engine[i])
        
        ################################################################################################################

        # 본명/영어이름
        try:
            driver.find_element_by_xpath('//*[@id="s_cd_real_name"]').clear()
            driver.find_element_by_xpath('//*[@id="s_cd_real_name"]').send_keys(real_name[i])
        except Exception as e:
            pass

        #여기까지 주석처리(에러 발생지점에서부터) command+/

        # 직업
        try :
            driver.find_element_by_xpath('//*[@id="s_cd_basic_info"]').clear()
            driver.find_element_by_xpath('//*[@id="s_cd_basic_info"]').send_keys(job[i])
        except Exception as e:
            pass
        # 키
        if pd.notnull(height[i]):
            try :
                driver.find_element_by_xpath('//*[@id="s_cd_height"]').clear()
                driver.find_element_by_xpath('//*[@id="s_cd_height"]').send_keys(str(height[i]))
            except Exception as e:
                pass
        # 몸무게
        if pd.notnull(weight[i]):
            try :
                driver.find_element_by_xpath('//*[@id="s_cd_weight"]').clear()
                driver.find_element_by_xpath('//*[@id="s_cd_weight"]').send_keys(str(weight[i]))
            except Exception as e:
                pass
        # 체질량 지수
        if pd.notnull(bmi[i]):
            try :
                driver.find_element_by_xpath('//*[@id="s_cd_bmi"]').clear()
                driver.find_element_by_xpath('//*[@id="s_cd_bmi"]').send_keys(str(bmi[i]))
            except Exception as e:
                pass
        # 체질량 지수 범위
        try :
            driver.find_element_by_xpath('//*[@id="s_cd_bmi_type"]').clear()
            driver.find_element_by_xpath('//*[@id="s_cd_bmi_type"]').send_keys(bmi_range[i])
        except Exception as e:
            pass
        # 양력생일
        if pd.notnull(sun_birth[i]):
            try :
                driver.find_element_by_xpath('//*[@id="s_cd_birth"]').clear()
                driver.find_element_by_xpath('//*[@id="s_cd_birth"]').send_keys(str(sun_birth[i]))
            except Exception as e:
                pass
        # 음력생일
        if pd.notnull(moon_birth[i]):
            try :
                driver.find_element_by_xpath('//*[@id="s_cd_lunar_birth"]').clear()
                driver.find_element_by_xpath('//*[@id="s_cd_lunar_birth"]').send_keys(str(moon_birth[i]))
            except Exception as e:
                pass
        # 별자리
        # if star[i] != '' :
        if pd.notnull(star[i]) :
            if star[i] == '게' :
                num = '2'
            elif star[i] == '물고기' :
                num = '3'
            elif star[i] == '물병' :
                num = '4'
            elif star[i] == '사수' :
                num = '5'
            elif star[i] == '사자' :
                num = '6'
            elif star[i] == '쌍둥이' :
                num = '7'
            elif star[i] == '양' :
                num = '8'
            elif star[i] == '염소' :
                num = '9'
            elif star[i] == '전갈' :
                num = '10'
            elif star[i] == '처녀' :
                num = '11'
            elif star[i] == '천칭' :
                num = '12'
            elif star[i] == '황소' :
                num = '13'
            else :
                pass
        else :
            num = '1'

        try :
            driver.find_element_by_xpath('//*[@id="s_cd_constellation"]').click()
            driver.find_element_by_xpath('//*[@id="s_cd_constellation"]/option[' + num + ']').click()
        except Exception as e:
                pass        

        # 띠
        # if animals[i] != '' :
        if pd.notnull(animals[i]) :
            if animals[i] == '개띠':
                num2 = '2'
            elif animals[i] == '닭띠':
                num2 = '3'
            elif animals[i] == '돼지띠':
                num2 = '4'
            elif animals[i] == '말띠':
                num2 = '5'
            elif animals[i] == '뱀띠':
                num2 = '6'
            elif animals[i] == '소띠':
                num2 = '7'
            elif animals[i] == '양띠':
                num2 = '8'
            elif animals[i] == '용띠':
                num2 = '9'
            elif animals[i] == '원숭이띠':
                num2 = '10'
            elif animals[i] == '쥐띠':
                num2 = '11'
            elif animals[i] == '토끼띠':
                num2 = '12'
            elif animals[i] == '호랑이띠':
                num2 = '13'
            else :
                pass
        else :
            num2 = '1'
        try :
            driver.find_element_by_xpath('//*[@id="s_cd_year_of"]').click()
            driver.find_element_by_xpath('//*[@id="s_cd_year_of"]/option[' + num2 + ']').click()
        except Exception as e:
            pass
            
        # 혈액형 
        if blood_type[i] == 'A':
            num_blood = '2'
        elif blood_type[i] == 'B':
            num_blood = '3'
        elif blood_type[i] == 'O':
            num_blood = '4'
        elif blood_type[i] == 'AB':
            num_blood = '5'
        else  :
            pass
        try :
            driver.find_element_by_xpath('//*[@id="s_cd_blood_type"]').click()
            driver.find_element_by_xpath('//*[@id="s_cd_blood_type"]/option[' + num_blood + ']').click()
        except Exception as e:
            pass
        # 출생지
        try :
            driver.find_element_by_xpath('//*[@id="s_cd_place_of_birth"]').clear()
            driver.find_element_by_xpath('//*[@id="s_cd_place_of_birth"]').send_keys(birth_site[i])
        except Exception as e:
            pass


        # 소속사
        try :
            driver.find_element_by_xpath('//*[@id="s_cd_agency"]').clear()
            driver.find_element_by_xpath('//*[@id="s_cd_agency"]').send_keys(sosogsa[i])
        except Exception as e:
            pass
        # 소속
        try :
            driver.find_element_by_xpath('//*[@id="s_cd_belong"]').clear()
            driver.find_element_by_xpath('//*[@id="s_cd_belong"]').send_keys(sosog[i])
        except Exception as e:
            pass
        # 소속팀
        try :
            driver.find_element_by_xpath('//*[@id="s_cd_team"]').clear()
            driver.find_element_by_xpath('//*[@id="s_cd_team"]').send_keys(sosog_team[i])
        except Exception as e:
            pass
        # 가족
        try :
            driver.find_element_by_xpath('//*[@id="s_cd_family"]').clear()
            driver.find_element_by_xpath('//*[@id="s_cd_family"]').send_keys(family[i])
        except Exception as e:
            pass
        # 그룹
        try :
            driver.find_element_by_xpath('//*[@id="s_cd_group"]').clear()
            # driver.find_element_by_xpath('//*[@id="s_cd_group"]').send_keys(group[i])
            driver.find_element_by_xpath('//*[@id="s_cd_group"]').send_keys(group[i])
        except Exception as e:
            pass
        # 데뷔
        try :
            driver.find_element_by_xpath('//*[@id="s_cd_debut"]').clear()
            # driver.find_element_by_xpath('//*[@id="s_cd_debut"]').send_keys(debut[i])
            driver.find_element_by_xpath('//*[@id="s_cd_debut"]').send_keys(debut[i])
        except Exception as e:
            pass

        # 그룹/팀 여부
        if group_flag[i] == 0:
            num_group = "1"
        elif group_flag[i] == 1:
            num_group = '2'
        else  :
            pass    
        try : 
            driver.find_element_by_xpath('//*[@id="s_cd_group_flag"]').click()

            driver.find_element_by_xpath('//*[@id="s_cd_group_flag"]/option[' + num_group + ']').click()
        except Exception as e:
            pass

        # 데뷔년도
        try :
            driver.find_element_by_xpath('//*[@id="s_cd_debut_year"]').clear()
            # driver.find_element_by_xpath('//*[@id="s_cd_debut_year"]').send_keys(debut[i][:4])
            driver.find_element_by_xpath('//*[@id="s_cd_debut_year"]').send_keys(debut[i][:4])
        except Exception as e:
            pass
        # 멤버
        try :
            driver.find_element_by_xpath('//*[@id="s_cd_members"]').clear()
            # driver.find_element_by_xpath('//*[@id="s_cd_members"]').send_keys(member[i])
            driver.find_element_by_xpath('//*[@id="s_cd_members"]').send_keys(member[i])
        except Exception as e:
            pass
        # 학력
        try :
            driver.find_element_by_xpath('//*[@id="s_cd_academic"]').clear()
            # driver.find_element_by_xpath('//*[@id="s_cd_academic"]').send_keys(student[i])
            driver.find_element_by_xpath('//*[@id="s_cd_academic"]').send_keys(student[i])
        except Exception as e:
            pass
        # 경력
        try:
            driver.find_element_by_xpath('//*[@id="s_cd_career"]').clear()
            # driver.find_element_by_xpath('//*[@id="s_cd_career"]').send_keys(career[i])
            driver.find_element_by_xpath('//*[@id="s_cd_career"]').send_keys(career[i])
        except Exception as e:
            pass
        # 수상경력
        try:
            driver.find_element_by_xpath('//*[@id="s_cd_award"]').clear()
            # driver.find_element_by_xpath('//*[@id="s_cd_award"]').send_keys(award[i])
            driver.find_element_by_xpath('//*[@id="s_cd_award"]').send_keys(award[i])
        except Exception as e:
            pass
        
        # --------------- 20/12/22 추가 부분(이지은) ------------------ #
        
        #그룹/솔로여부(입력값:group,member,solo)
        if group_flag[i] == 0 and group[i] is np.nan:
            group_type = 'solo'
        elif group_flag[i] == 0 and group[i] is not np.nan:
            group_type = 'member'
        elif group_flag[i] == 1:
            group_type = 'group'
        else:
            pass

        # if group_flag[i] == 0 and np.isnan(group[i]):
        #     group_type = 'solo'
        # elif group_flag[i] == 0 and ~np.isnan(group[i]):
        #     group_type = 'member'
        # elif group_flag[i] == 1:
        #     group_type = 'group'
        # else:
        #     pass

        try :
            driver.find_element_by_xpath('//*[@id="s_cd_group_type"]').clear()
            driver.find_element_by_xpath('//*[@id="s_cd_group_type"]').send_keys(group_type)
        except Exception as e:
            pass
        
        # 프로필 hex code(상단값,하단값 2개입력)
        list_group = ["['#755af5', '#ff70a9']", "['#308bf0', '#6537f8']"]
        list_female = ["['#0087f7', '#c979ff']", "['#f13092', '#ff8080']"]
        list_male = ["['#3742e5', '#b57efc']", "['#fb6d75', '#ffbf61']"]

        if group_flag[i] == 1 :
            color_hex_group = random.choice(list_group)
        elif sex[i] == '남자' or sex[i] == '남성':
            color_hex_group = random.choice(list_male)
        elif sex[i] == '여자' or sex[i] == '여성':
            color_hex_group = random.choice(list_female)
        else:
            color_hex_group = "['#755af5', '#ff70a9']"
    
        try :
            driver.find_element_by_xpath('//*[@id="s_color_hex_group"]').clear()
            driver.find_element_by_xpath('//*[@id="s_color_hex_group"]').send_keys(color_hex_group)
        except Exception as e:
            pass
        
        # 프로필 상단 색상값(미사용)
        if group_flag[i] == 1 :
            cd_profile_color = 2
        elif sex[i] == '남자' or sex[i] == '남성':
            cd_profile_color = 3
        elif sex[i] == '여자' or sex[i] == '여성':
            cd_profile_color = 0
        else :
            pass

        try :
            driver.find_element_by_xpath('//*[@id="s_cd_profile_color"]').clear()
            driver.find_element_by_xpath('//*[@id="s_cd_profile_color"]').send_keys(cd_profile_color)
        except Exception as e:
            pass
        
        # 소속사 idx
        # conn = db_connection()
        # result = pd.read_sql('''
        #     SELECT *
        #     FROM agency_data
        #     ;
        #  ''',con=conn)
        #
        # li_ad_name = list(np.array(result['ad_name'].tolist()))
        # try :
        #     idx = li_ad_name.index(sosogsa[i])
        #     agency_idx = result['cd_idx'][idx]
        #     try :
        #         driver.find_element_by_xpath('//*[@id="s_agency_idx"]').clear()
        #         driver.find_element_by_xpath('//*[@id="s_agency_idx"]').send_keys(int(agency_idx))
        #     except Exception as e:
        #         pass
        # except :
        #     pass
        
        # profile_url
        if group_flag[i] == 1:
            cd_type = 'group'
        elif sex[i] == '여자':
            cd_type = 'woman'
        elif sex[i] == '남자':
            cd_type = 'man'

        profile_url_main = 'https://all.image.mycelebs.com/{cd_type}/{cd_type}_1125@1464.jpg'.format(cd_type=cd_type)
        try:
            driver.find_element_by_xpath('//*[@id="s_profile_url_main"]').clear()
            driver.find_element_by_xpath('//*[@id="s_profile_url_main"]').send_keys(profile_url_main)
        except Exception as e:
            pass

        profile_url_rectangle_small = 'https://all.image.mycelebs.com/{cd_type}/{cd_type}_1035@420.jpg'.format(
            cd_type=cd_type)
        try:
            driver.find_element_by_xpath('//*[@id="s_profile_url_rectangle_small"]').clear()
            driver.find_element_by_xpath('//*[@id="s_profile_url_rectangle_small"]').send_keys(
                profile_url_rectangle_small)
        except Exception as e:
            pass

        profile_url_diagonal = 'https://all.image.mycelebs.com/{cd_type}/{cd_type}_573@372.jpg'.format(cd_type=cd_type)
        try:
            driver.find_element_by_xpath('//*[@id="s_profile_url_diagonal"]').clear()
            driver.find_element_by_xpath('//*[@id="s_profile_url_diagonal"]').send_keys(profile_url_diagonal)
        except Exception as e:
            pass

        profile_url_square_medium = 'https://all.image.mycelebs.com/{cd_type}/{cd_type}_288@288.jpg'.format(
            cd_type=cd_type)
        try:
            driver.find_element_by_xpath('//*[@id="s_profile_url_square_medium"]').clear()
            driver.find_element_by_xpath('//*[@id="s_profile_url_square_medium"]').send_keys(profile_url_square_medium)
        except Exception as e:
            pass

        profile_url_square_small = 'https://all.image.mycelebs.com/{cd_type}/{cd_type}_240@240.jpg'.format(
            cd_type=cd_type)
        try:
            driver.find_element_by_xpath('//*[@id="s_profile_url_square_small"]').clear()
            driver.find_element_by_xpath('//*[@id="s_profile_url_square_small"]').send_keys(profile_url_square_small)
        except Exception as e:
            pass

        profile_url_round_large = 'https://all.image.mycelebs.com/{cd_type}/{cd_type}_249@249.jpg'.format(
            cd_type=cd_type)
        try:
            driver.find_element_by_xpath('//*[@id="s_profile_url_round_large"]').clear()
            driver.find_element_by_xpath('//*[@id="s_profile_url_round_large"]').send_keys(profile_url_round_large)
        except Exception as e:
            pass

        profile_url_round_medium = 'https://all.image.mycelebs.com/{cd_type}/{cd_type}_144@144.jpg'.format(
            cd_type=cd_type)
        try:
            driver.find_element_by_xpath('//*[@id="s_profile_url_round_medium"]').clear()
            driver.find_element_by_xpath('//*[@id="s_profile_url_round_medium"]').send_keys(profile_url_round_medium)
        except Exception as e:
            pass

        profile_url_round_small = 'https://all.image.mycelebs.com/{cd_type}/{cd_type}_108@108.jpg'.format(
            cd_type=cd_type)
        try:
            driver.find_element_by_xpath('//*[@id="s_profile_url_round_small"]').clear()
            driver.find_element_by_xpath('//*[@id="s_profile_url_round_small"]').send_keys(profile_url_round_small)
        except Exception as e:
            pass

        profile_url_rectangle_large = 'https://all.image.mycelebs.com/{cd_type}/{cd_type}_1035@738.jpg'.format(
            cd_type=cd_type)
        try:
            driver.find_element_by_xpath('//*[@id="s_profile_url_rectangle_large"]').clear()
            driver.find_element_by_xpath('//*[@id="s_profile_url_rectangle_large"]').send_keys(
                profile_url_rectangle_large)
        except Exception as e:
            pass

        profile_url_square_large = 'https://all.image.mycelebs.com/{cd_type}/{cd_type}_420@420.jpg'.format(
            cd_type=cd_type)
        try:
            driver.find_element_by_xpath('//*[@id="s_profile_url_square_large"]').clear()
            driver.find_element_by_xpath('//*[@id="s_profile_url_square_large"]').send_keys(profile_url_square_large)
        except Exception as e:
            pass

        # ----------------------------------------------- #
        

        # 인물 검사시 매칭 단어
        # driver.find_element_by_css_selector()

        # 21/10/22 name[i] -> match_word[i] 로 수정
        if pd.isnull(match_word[i]) :
            match_word[i] = name[i]

        driver.find_element_by_xpath('//*[@id="s_cd_match_word"]').clear()
        driver.find_element_by_xpath('//*[@id="s_cd_match_word"]').send_keys(match_word[i])

        # 사망 여부
        if death[i] == 1:
            num_death = "1"
            try:
                driver.find_element_by_xpath('//*[@id="s_cd_is_dead"]').click()
                driver.find_element_by_xpath('//*[@id="s_cd_is_dead"]/option[' + num_death + ']').click()
            except Exception as e:
                pass

        # --------------- 21/10/21 ------------------ #

        # 네이버 프로필 고유값
        if pd.notnull(naver_people_id[i]) :
            driver.find_element_by_xpath('//*[@id="s_naver_people_id"]').clear()
            driver.find_element_by_xpath('//*[@id="s_naver_people_id"]').send_keys(str(naver_people_id[i]))

        # 네이버 영화 프로필 고유값
        if pd.notnull(naver_movie_people_id[i]):
            driver.find_element_by_xpath('//*[@id="s_naver_movie_people_id"]').clear()
            driver.find_element_by_xpath('//*[@id="s_naver_movie_people_id"]').send_keys(str(naver_movie_people_id[i]))

        # 다음 프로필 고유값
        if pd.notnull(daum_people_id[i]) :
            driver.find_element_by_xpath('//*[@id="s_daum_people_id"]').clear()
            driver.find_element_by_xpath('//*[@id="s_daum_people_id"]').send_keys(daum_people_id[i])

        # 광고정보센터 고유값
        if pd.notnull(adic_people_id[i]):
            driver.find_element_by_xpath('//*[@id="s_adic_people_id"]').clear()
            driver.find_element_by_xpath('//*[@id="s_adic_people_id"]').send_keys(str(adic_people_id[i]))

        # 플레이디비 고유값
        if pd.notnull(playdb_people_id[i]):
            driver.find_element_by_xpath('//*[@id="s_playdb_people_id"]').clear()
            driver.find_element_by_xpath('//*[@id="s_playdb_people_id"]').send_keys(str(playdb_people_id[i]))

        # 바이브 고유값
        if pd.notnull(vibe_people_id[i]):
            driver.find_element_by_xpath('//*[@id="s_vibe_people_id"]').clear()
            driver.find_element_by_xpath('//*[@id="s_vibe_people_id"]').send_keys(str(vibe_people_id[i]))

        # -------------------------------------------- #

        # driver.find_element_by_css_selector("#s_cd_match_word").send_keys(name[i])
        # 사용 여부 - 공개와 수집 요녀석은 픽스지 ㅎㅎ
        
        driver.find_element_by_xpath('//*[@id="s_cd_is_use"]').click()
        driver.find_element_by_xpath('//*[@id="s_cd_is_use"]/option[3]').click()  # 공개수집
        
        # 사용 여부 - 미공개 , 테스트용
        # driver.find_element_by_xpath('//*[@id="s_cd_is_use"]').click()
        # driver.find_element_by_xpath('//*[@id="s_cd_is_use"]/option[1]').click() # 미공개
        
        # 추가버튼!!!! 확인하고 돌리자
        driver.find_element_by_xpath('//*[@id="write"]/div/div/div[2]/div/input').click()
        # 정상처리 되었습니다.
        time.sleep(1)
        alert.accept()
        
        ####################################################################################################
        # 사이트 등록하기 - 추가버튼 누르고 나서야 생성되는 버튼이다 # 공식사이트만하자.. 일단은...
        # 공식사이트
        if pd.notnull(site_1[i]) :
            driver.find_element_by_xpath('//*[@id="site_type"]').click()
            driver.find_element_by_xpath('//*[@id="site_type"]/option[2]').click()
            try :
                driver.find_element_by_xpath('//*[@id="site_url"]').clear()
                driver.find_element_by_xpath('//*[@id="site_url"]').send_keys(site_1[i])
                driver.find_element_by_xpath('//*[@id="add_site_btn"]').click()
                time.sleep(1)
            except Exception as e:
                pass
        # 팬카페
        if pd.notnull(site_2[i]) :
            driver.find_element_by_xpath('//*[@id="site_type"]').click()
            driver.find_element_by_xpath('//*[@id="site_type"]/option[3]').click()
            try :
                driver.find_element_by_xpath('//*[@id="site_url"]').clear()
                driver.find_element_by_xpath('//*[@id="site_url"]').send_keys(site_2[i])
                driver.find_element_by_xpath('//*[@id="add_site_btn"]').click()
                time.sleep(1)
            except Exception as e:
                pass
        # 블로그
        if pd.notnull(site_3[i]) :
            driver.find_element_by_xpath('//*[@id="site_type"]').click()
            driver.find_element_by_xpath('//*[@id="site_type"]/option[4]').click()
            try :
                driver.find_element_by_xpath('//*[@id="site_url"]').clear()
                driver.find_element_by_xpath('//*[@id="site_url"]').send_keys(site_3[i])
                driver.find_element_by_xpath('//*[@id="add_site_btn"]').click()
                time.sleep(1)
            except Exception as e:
                pass
        # 미니홈피
        if pd.notnull(site_4[i]) :
            driver.find_element_by_xpath('//*[@id="site_type"]').click()
            driver.find_element_by_xpath('//*[@id="site_type"]/option[5]').click()
            try :
                driver.find_element_by_xpath('//*[@id="site_url"]').clear()
                driver.find_element_by_xpath('//*[@id="site_url"]').send_keys(site_4[i])
                driver.find_element_by_xpath('//*[@id="add_site_btn"]').click()
                time.sleep(1)
            except Exception as e:
                pass
        # 트위터
        if pd.notnull(site_5[i]) :
            driver.find_element_by_xpath('//*[@id="site_type"]').click()
            driver.find_element_by_xpath('//*[@id="site_type"]/option[6]').click()
            try :
                driver.find_element_by_xpath('//*[@id="site_url"]').clear()
                driver.find_element_by_xpath('//*[@id="site_url"]').send_keys(site_5[i])
                driver.find_element_by_xpath('//*[@id="add_site_btn"]').click()
                time.sleep(1)
            except Exception as e:
                pass
        # 페이스북
        if pd.notnull(site_6[i]) :
            driver.find_element_by_xpath('//*[@id="site_type"]').click()
            driver.find_element_by_xpath('//*[@id="site_type"]/option[7]').click()
            try :
                driver.find_element_by_xpath('//*[@id="site_url"]').clear()
                driver.find_element_by_xpath('//*[@id="site_url"]').send_keys(site_6[i])
                driver.find_element_by_xpath('//*[@id="add_site_btn"]').click()
                time.sleep(1)
            except Exception as e:
                pass
        # 인스타그램
        if pd.notnull(site_7[i]) :
            driver.find_element_by_xpath('//*[@id="site_type"]').click()
            driver.find_element_by_xpath('//*[@id="site_type"]/option[8]').click()
            try :
                driver.find_element_by_xpath('//*[@id="site_url"]').clear()
                driver.find_element_by_xpath('//*[@id="site_url"]').send_keys(site_7[i])
                driver.find_element_by_xpath('//*[@id="add_site_btn"]').click()
                time.sleep(1)
            except Exception as e:
                pass
        # 유투브
        if pd.notnull(site_8[i]) :
            driver.find_element_by_xpath('//*[@id="site_type"]').click()
            driver.find_element_by_xpath('//*[@id="site_type"]/option[9]').click()
            try :
                driver.find_element_by_xpath('//*[@id="site_url"]').clear()
                driver.find_element_by_xpath('//*[@id="site_url"]').send_keys(site_8[i])
                driver.find_element_by_xpath('//*[@id="add_site_btn"]').click()
                time.sleep(1)
            except Exception as e:
                pass
        # 웨이보
        if pd.notnull(site_9[i]) :
            driver.find_element_by_xpath('//*[@id="site_type"]').click()
            driver.find_element_by_xpath('//*[@id="site_type"]/option[10]').click()
            try :
                driver.find_element_by_xpath('//*[@id="site_url"]').clear()
                driver.find_element_by_xpath('//*[@id="site_url"]').send_keys(site_9[i])
                driver.find_element_by_xpath('//*[@id="add_site_btn"]').click()
                time.sleep(1)
            except Exception as e:
                pass
        # 브이라이브
        if pd.notnull(site_10[i]) :
            driver.find_element_by_xpath('//*[@id="site_type"]').click()
            driver.find_element_by_xpath('//*[@id="site_type"]/option[15]').click()
            try :
                driver.find_element_by_xpath('//*[@id="site_url"]').clear()
                driver.find_element_by_xpath('//*[@id="site_url"]').send_keys(site_10[i])
                driver.find_element_by_xpath('//*[@id="add_site_btn"]').click()
                time.sleep(1)
            except Exception as e:
                pass

    driver.close()
