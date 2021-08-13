'''

목표 : kobis_people_list 내 데이터와 celeb 내 데이터를 "자동으로" 매핑하자 !
매핑 대상 : 배우/감독/조감독이고, 네이버 프로필이 있고, 한국인인 사람  (대상은 바뀔 수 있음)
주의 사항 : 네이버 크롤링 할 때 조금씩 나눠서 수집하고, VPN 켜서 하기..!

매핑 프로세스 :
    1. celeb에 동명이인이 있는 경우
        1.1 동명이인이 있고 kobis_people_list에 생일 데이터가 있는 경우   <1차 매핑> - mapping_1()
            (1) 이름과 생일이 모두 일치하면 자동 매핑
            (2) 이름과 생일이 일치하지 않으면 신규 등록 대상

        1.2 동명이인이 있고 kobis_people_list에 생일 데이터가 없는 경우   <2차 매핑>
            -> 비교해야할 생일 데이터가 없으므로 네이버 프로필을 타고 가서 각종 정보를 크롤링해오고 celeb 데이터와 비교, 매핑함  -  naver_crawling(), mapping_2()
            -> 비교하는 정보는 성별, 생일, 데뷔 정보, 경력사항, 학력, 수상내역, 키 & 몸무게 정보임
            -> 비교하는 정보가 일치하면 자동으로 매핑이 되고, 정보가 다르면 신규 등록 대상, 정보가 없어서 비교 불가하면 수기 매핑 대상으로 분류됨
            -> 매핑이 된 개체와 신규 등록해야하는 개체, 수기 매핑해야 하는 개체, 새로 insert 되어서 아직 매핑 프로세스를 거치지 않은 개체를 구분하기 위해
                kobis_people_list 에 'mapping' 컬럼을 만들어서 관리함
            -> 매핑이 된 개체 : mapping=1     신규 등록 대상 : mapping=0    수기 매핑 대상 : mapping=2    새로 insert된 개체 : mapping=NULL
            -> 'mapping'은 코드 돌리면 kobis_people_list에 자동으로 업데이트 됨!  - db_update_1(), db_update_2()
            -> 자동으로 매핑된 개체 또한 kuk.movie_people_connection 에 insert 됨

    2. celeb에 동명이인이 없는 경우
        -> 신규 등록 대상

코드 돌리고 나서 :
-> kobis_people_list 내 매핑 기준에 따른 개체들을 대상으로 자동 매핑이 되고,
-> 매핑이 된 개체(mapping=1), 신규 등록 대상(mapping=0), 수기 매핑 대상(mapping=2)으로 자동 분류됨
-> 후에 mapping=2 만 불러와서 수기로 매핑을 하고  (3_KOBIS_수기매핑.ipynb)
-> mapping=0 만 불러와서 인물등록을 진행함       (4_KOBIS_인물등록.py)


+ (21/07/26) 현재까지 이슈 사항
-> 코비스에 중복으로 등록되어 있는 인물이 쫌,, 아니 아주 많음,,
-> 초기에 mapping 분류를 다 해버렸더니 이미 신규 등록한 인물이 인물등록 리스트에 또 노출됨 (같은 인물인데 피플 코드가 다른 사람)
-> 따라서 mapping 분류를 다 했어도, 어느 정도 신규 인물 등록이 진행된 상황이라면 mapping=1과 mapping=-1만 유지하고
-> mapping=0 은 null 로 바꾸고 다시 매핑하는게 좋을 듯함


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

# db connect
def db_connection(host_name='ds'):
    host_url = "db.ds.mycelebs.com"
    user_nm = "j_eungg"
    passwd = "mycelebs"
    port_num = 3306
    conn = pymysql.connect(host=host_url, user=user_nm, passwd=passwd, port = port_num, charset='utf8',
                           cursorclass=pymysql.cursors.DictCursor)
    # cursorclass=pymysql.cursors.DictCursor 추가 -> 데이터프레임으로 다루기 쉽게 딕셔너리 형태로 결과 반환해줌, cursor는 튜플형태
    # db=db,
    engine = create_engine(f'mysql+pymysql://{user_nm}:{passwd}@{host_url}:{port_num}/kuk?charset=utf8mb4')
    return conn, engine



# data load
def getPeople():
    conn, engine = db_connection()

    query = "select * from maimovie_kr.kobis_people_list"
    kobis = pd.read_sql(query, con=conn)

    kobis = kobis[(kobis['repRoleNm'] == '배우') | (kobis['repRoleNm'] == '감독') | (kobis['repRoleNm'] == '조감독')]      # 배우, 감독, 조감독인 사람들
    kobis = kobis.drop_duplicates(subset='peopleCd', keep='first')      # 중복 제거
    kobis = kobis.dropna(subset=['profile_naverURL'])       # 네이버 url 있는 사람들

    # 이름 글자 수가 5이상이면 외국인이라고 판단....?
    kobis['nm_num'] = kobis.peopleNm.str.len()      # 이름 글자 수 추출
    kobis = kobis[kobis['nm_num'] <= 4]         # 한국인만 추출

    total_kobis = kobis      # total 매핑 대상

    # 매핑 된 사람들(mapping = 1)
    kobis_mapping = kobis[kobis['mapping'] == 1]

    # 매핑 안된 사람들(mapping = 0, 2, null)
    idx = kobis[kobis['mapping'] == 1].index
    idx2 = kobis[kobis['mapping'] == -1].index
    kobis_not_mapping = kobis.drop(idx, inplace=False)
    kobis_not_mapping = kobis.drop(idx2, inplace=False)

    # 아무것도 안된 사람들(mapping = null)
    kobis_list = kobis[kobis['mapping'].isnull()]

    conn.close()

    print("[INFO] data load DONE!\n")
    print("[INFO] 새로 매핑해야 하는 개체는 {} 명 입니다.".format(len(kobis_list)))

    return total_kobis, kobis_mapping, kobis_not_mapping, kobis_list



def getCelebPeople() :
    conn, engine = db_connection()

    query = "SELECT * FROM star_ko.star_ko_data WHERE (cd_category LIKE '%감독%' OR cd_category LIKE '%배우%' OR cd_category LIKE '%방송인%' OR cd_category LIKE '%탤런트%' OR cd_category LIKE '%작가%' OR cd_category LIKE '%PD%' OR cd_category LIKE '%연출%') and cd_is_use=1"
    celeb = pd.read_sql(query, con=conn)

    conn.close()

    return celeb



# 동명이인 유무
def get_mapping_data() :

    conn, engine = db_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    celeb_list1 = celeb['cd_name'].to_list()
    celeb_list2 = celeb['cd_real_name'].to_list()
    celeb_list2 = [v for v in celeb_list2 if v]     # 빈 문자열 제거
    celeb_list3 = []
    for c in celeb_list2:
        celeb_list3.extend(c.split('/'))

    celeb_list = celeb_list1 + celeb_list3      # 이름과 본명으로 이루어진 리스트

    # 동명이인 있는 개체들 -> 생일 데이터로 매핑
    sameExist = kobis_list[kobis_list['peopleNm'].isin(celeb_list)]
    # 동명이인 없는 개체들 -> 인물등록
    sameNonExist = kobis_list[kobis_list['peopleNm'].isin(celeb_list) == False]

    # 1. 동명이인 개체들 생일 데이터로 매핑
    # 1.1 생일 데이터 없는 개체들(2차 매핑 대상)
    birthNonExist_1 = sameExist[sameExist['birth'].isnull()]
    birthNonExist_2 = sameExist[sameExist['birth'] == '0000-00-00']
    birthNonExist_3 = sameExist[sameExist['birth'] == '']
    birthNonExist = pd.concat([birthNonExist_1, birthNonExist_2, birthNonExist_3])

    # 1.2 생일 데이터 있는 개체들(1차 매핑 대상)
    idx = birthNonExist.index
    birthExist = sameExist.drop(idx, inplace=False)


    # 2. 동명이인 없는 개체들 mapping=0 으로 업데이트(인물등록)
    sameNonExist_list = sameNonExist['peopleCd'].to_list()

    if len(sameNonExist_list) > 0 :
        query = "UPDATE maimovie_kr.kobis_people_list SET mapping=0 WHERE peopleCd=%s;"
        cursor.executemany(query, sameNonExist_list)
        conn.commit()
        print("[INFO] 동명이인 없는 개체 {} 명 -> mapping = 0 db update 완료".format(len(sameNonExist_list)))

    else :
        # print("[INFO] 동명이인 없는 개체 없음")
        pass

    conn.close()

    return birthExist, birthNonExist


# 1차 매핑 -> 생일 데이터 있는 개체들
def mapping_1() :

    conn, engine = db_connection()

    mapping_1 = []
    newPeople_1 = []
    checking_1 = []
    checking__1 = []

    print("[INFO] 1차 매핑 시작 ... ")
    for e, row in birthExist.iterrows():

        peopleCode = row['peopleCd']
        name = row['peopleNm']
        gender = row['sex']
        birth = row['birth']

        query = f"select series_id, cd_name, cd_gender, cd_category, cd_birth, cd_debut, cd_height, cd_weight from star_ko.star_ko_data where (cd_name='{name}' or cd_real_name='{name}') and (cd_category LIKE '%감독%' OR cd_category LIKE '%배우%' OR cd_category LIKE '%방송인%' OR cd_category LIKE '%탤런트%' OR cd_category LIKE '%작가%' OR cd_category LIKE '%PD%' OR cd_category LIKE '%연출%') and cd_is_use=1"
        celebData = pd.read_sql(query, con=conn)
        celebData = celebData.fillna('')

        try:
            for c in range(len(celebData)):
                series_id = celebData['series_id'][c]
                cd_name = celebData['cd_name'][c]
                cd_gender = celebData['cd_gender'][c]
                cd_birth = celebData['cd_birth'][c]

                if cd_birth == '0000-00-00 00:00:00':
                    cd_birth_ = ''
                elif cd_birth == '':
                    cd_birth_ = ''
                elif pd.isnull(cd_birth) :
                    cd_birth_ = ''
                else:
                    cd_birth_ = cd_birth.strftime('%Y-%m-%d')

                # if len(birth) == 4:
                #     cd_birth_ = cd_birth_[:4]


                # 생일 데이터 없으면 -> 수기매핑
                if birth == '' or cd_birth_ == '' :
                    checking_1.append(peopleCode)
                    print("[INFO] 비교 데이터 부족! ")
                    raise NotImplementedError

                # 생일 같으면 -> 매핑
                # elif birth != '' and birth == cd_birth_:
                elif birth == cd_birth_:
                    mapping_data = {'kobis_people_id': peopleCode, 'celeb_idx': int(series_id), 'cd_name': cd_name}
                    mapping_1.append(mapping_data)

                    print("---------- 크롤링 데이터 ----------")
                    print(peopleCode)
                    print("이름: ", name, "| 성별: ", gender, "| 생일: ", birth)

                    print("---------- 셀럽 데이터 ------------")
                    print(series_id)
                    print("이름: ", cd_name, "| 성별: ", cd_gender, "| 생일: ", cd_birth_)

                    print(name, " 매핑 성공!")
                    raise NotImplementedError

                # 다르면 -> 인물등록
                else :
                    newPeople_1.append(peopleCode)
                    print(name, " 매핑 실패!")
                    raise NotImplementedError
        except:
            pass

    print("[INFO] 1차 매핑 완료 ... \n")

    mapping_1_df = pd.DataFrame(mapping_1, columns=['kobis_people_id', 'celeb_idx', 'cd_name'])
    mapping_1_list = mapping_1_df['kobis_people_id'].to_list()

    # 중복 제거
    newPeople_1 = list(set(newPeople_1))
    checking_1 = list(set(checking_1))

    for i in checking_1:
        if i not in mapping_1_list:
            checking__1.append(i)

    print('*{0}명 중 {1}명 매핑 성공하였습니다.'.format(len(birthExist), len(mapping_1)))
    print('*{0}명 중 {1}명 매핑 실패하였습니다. 인물등록을 진행하세요.'.format(len(birthExist), len(newPeople_1)))
    print('*{0}명 중 {1}명 매핑 실패하였습니다. 수기 매핑을 진행하세요. \n'.format(len(birthExist), len(checking__1)))

    conn.close()

    return mapping_1_df, newPeople_1, checking__1



def db_update_1(mapping_1_df) :
    conn, engine = db_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    duplicated_list1 = []

    # 맵핑된 개체들 movie_people_connection db insert
    if len(mapping_1_df) > 0 :
        # mapping_1_df = mapping_1_df.astype({'kobis_people_id': 'int'})
        # mapping_1_df['update_date'] = data_date
        # mapping_1_df.to_sql('movie_people_connection', engine, if_exists='append', index=None)
        # print("[INFO] 1차 매핑 결과 -> movie_people_connection db insert 완료")

        # 중복 celeb_idx 있는 경우 connection에 insert하지 않음
        for i, row in mapping_1_df.iterrows():
            kobis_people_id = row['kobis_people_id']
            celeb_idx = row['celeb_idx']
            cd_name = row['cd_name']

            qry1 = f'select * from kuk.movie_people_connection where celeb_idx={celeb_idx}'
            result1 = pd.read_sql(qry1, conn)

            if len(result1) == 0:
                qry2 = f'INSERT INTO kuk.movie_people_connection (kobis_people_id, celeb_idx, cd_name, update_date) values ({kobis_people_id}, {celeb_idx}, \'{cd_name}\', NOW())'
                cursor.execute(qry2)
            else:
                duplicated1 = {'kobis_people_id': kobis_people_id, 'celeb_idx': celeb_idx, 'cd_name': cd_name}
                duplicated_list1.append(duplicated1)
                print('! 중복된 celeb_idx 가 있습니다. kobis_people_id: {0}, celeb_idx: {1}, cd_name: {2}'.format(kobis_people_id, celeb_idx, cd_name))

        print("[INFO] 1차 매핑 결과 -> movie_people_connection db insert 완료")
        conn.commit()

        # mapping 여부 db update
        mapping_1_list = mapping_1_df['kobis_people_id'].to_list()
        mapping_query = "UPDATE maimovie_kr.kobis_people_list SET mapping=1 WHERE peopleCd=%s;"
        cursor.executemany(mapping_query, mapping_1_list)
        conn.commit()
        print("[INFO] 1차 매핑 결과 -> mapping = 1 db update 완료")

    # mapping 여부 db update
    if len(newPeople_1) > 0 :
        not_mapping_1_list = newPeople_1
        not_mapping_query = "UPDATE maimovie_kr.kobis_people_list SET mapping=0 WHERE peopleCd=%s;"
        cursor.executemany(not_mapping_query, not_mapping_1_list)
        conn.commit()
        print("[INFO] 1차 매핑 결과 -> mapping = 0 db update 완료")

    if len(checking__1) > 0 :
        checking_list_1 = checking__1
        checking_1_mapping_query = "UPDATE maimovie_kr.kobis_people_list SET mapping=2 WHERE peopleCd=%s;"
        cursor.executemany(checking_1_mapping_query, checking_list_1)
        conn.commit()
        print("[INFO] 1차 매핑 결과 -> mapping = 2 db update 완료 \n")

    conn.close()

    return duplicated_list1


# 2차 매핑 -> 생일 데이터 없는 개체들 -> 네이버 크롤링 후 정보 비교
def naver_crawling(number) :

    conn, engine = db_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    ex = birthNonExist.iloc[:number, :]

    empty_frame = pd.DataFrame(
        columns=('peopleCd', 'peopleNm', 'sex', 'repRoleNm', 'birth', 'debut', 'height', 'weight', 'career', 'academic', 'awards'))

    for i, row in tqdm(ex.iterrows(), total=ex.shape[0]):
        peopleCode = name = gender = job = birth = debut = height = weight = col_career = col_academic = col_awards = ''
        year, month, day = '', '', ''

        career_ = []
        academic_ = []
        awards_ = []

        peopleCode = row['peopleCd']
        name = row['peopleNm']
        gender = row['sex']
        url = row['profile_naverURL']

        naver_html = requests.get(url).text
        naver_soup = BeautifulSoup(naver_html, 'html.parser')

        profile_dsc = naver_soup.select('div[class=profile_dsc]')

        if job == '':
            try:
                job = profile_dsc[0].select('dd[class=sub]')[0].text
            except:
                pass

        dt_in_profile_dsc_dsc = profile_dsc[0].select('dl[class=dsc]')[0].select('dt')
        dd_in_profile_dsc_dsc = profile_dsc[0].select('dl[class=dsc]')[0].select('dd')

        for row_len in range(len(dt_in_profile_dsc_dsc)):
            title = dt_in_profile_dsc_dsc[row_len].text
            inner = dd_in_profile_dsc_dsc[row_len].text
            inner2 = re.findall('[0-9]+', inner)
            if '출생' in title:
                if len(inner2) == 3:
                    b = '-'.join(inner2)
                    a = datetime.datetime.strptime(b, '%Y-%m-%d')
                    birth = a.strftime('%Y-%m-%d')
                elif len(inner2) == 1:
                    birth = inner2[0]

            elif '데뷔' in title:
                debut = inner.strip()

            elif '신체' in title:
                tmp_body = inner.split(',')
                for it_tmp_body in tmp_body:
                    if 'cm' in it_tmp_body:
                        if height == '':
                            height = re.sub("\D", "", it_tmp_body)
                    elif 'kg' in it_tmp_body:
                        if weight == '':
                            weight = re.sub("\D", "", it_tmp_body)

        h3_in_profile_dsc_dsc = naver_soup.select('div[class=record_wrap]')[0].select('h3[class=blind]')
        d1_in_profile_dsc_dsc = naver_soup.select('div[class=record_wrap]')[0].select('dl')

        # for v in range(len(h3_in_profile_dsc_dsc)):
        for v in range(len(d1_in_profile_dsc_dsc)):
            title = h3_in_profile_dsc_dsc[v].text
            inner = d1_in_profile_dsc_dsc[v].select('p')

            if '경력' in title:
                for j in inner:
                    career = j.text
                    career_.append(career)
                    col_career = '|'.join(career_)

            elif '학력' in title:
                for j in inner:
                    academic = j.text
                    academic_.append(academic)
                    col_academic = '|'.join(academic_)

            elif '수상' in title:
                for j in inner:
                    awards = j.text
                    awards_.append(awards)
                    col_awards = '|'.join(awards_)

        final_list = [peopleCode, name, gender, job, birth, debut, height, weight, col_career, col_academic, col_awards]

        for final in final_list:
            if '\t' in final:
                final = final.replace('\t', '')

        empty_frame.loc[i] = final_list

        if birth != '':
            query = f"update maimovie_kr.kobis_people_list set birth = '{birth}'  where peopleCd = {peopleCode}"
            cursor.execute(query)
            conn.commit()

    print("[INFO] crawling DONE")
    return empty_frame


def mapping_2() :
    conn, engine = db_connection()

    mapping_2 = []
    newPeople_2 = []
    checking = []
    checking_ = []
    checking__ = []

    print("[INFO] 2차 매핑 시작 ...")

    for p, row in empty_frame.iterrows():

        peopleCode = row['peopleCd']
        name = row['peopleNm']
        gender = row['sex']
        job = row['repRoleNm']
        birth = row['birth']
        debut = row['debut']
        debut = re.sub("\!|\'|\?", "", str(debut))
        height = row['height']
        weight = row['weight']
        career = row['career']
        academic = row['academic']
        awards = row['awards']

        print("---------- 크롤링 데이터 ----------")
        print(peopleCode)
        print("이름: ", name, "| 성별: ", gender, "| 직업: ", job, "| 생일: ", birth, "| 데뷔:", debut, "| 키: ", height, "| 몸무게:",
              weight)

        query = f'''select * from star_ko.star_ko_data where (cd_name='{name}' or cd_real_name='{name}')
                    and (cd_category LIKE '%감독%' OR cd_category LIKE '%배우%' OR cd_category LIKE '%방송인%' OR cd_category LIKE '%탤런트%' OR cd_category LIKE '%작가%' OR cd_category LIKE '%PD%' OR cd_category LIKE '%연출%') and cd_is_use=1'''
        # query = f"select * from star_ko.star_ko_data where cd_name='{name}'"
        celebData = pd.read_sql(query, con=conn)
        celebData = celebData.fillna('')

        try:
            for c in range(len(celebData)):
                series_id = celebData['series_id'][c]
                cd_name = celebData['cd_name'][c]
                cd_gender = celebData['cd_gender'][c]
                cd_category = celebData['cd_category'][c]
                cd_birth = celebData['cd_birth'][c]
                if cd_birth == '0000-00-00 00:00:00':
                    cd_birth_ = ''
                elif cd_birth == '':
                    cd_birth_ = ''
                else:
                    cd_birth_ = cd_birth.strftime('%Y-%m-%d')

                # if len(birth) == 4:
                #     cd_birth_ = cd_birth_[:4]

                cd_debut = celebData['cd_debut'][c]
                cd_debut = re.sub("\!|\'|\?", "", str(cd_debut))
                cd_height = celebData['cd_height'][c]
                cd_weight = celebData['cd_weight'][c]
                cd_career = celebData['cd_career'][c]
                cd_academic = celebData['cd_academic'][c]
                cd_award = celebData['cd_award'][c]

                print("---------- 셀럽 데이터 ------------")
                print(series_id)
                print("이름: ", cd_name, "| 성별: ", cd_gender, "| 직업: ", cd_category, "| 생일: ", cd_birth_, "| 데뷔:",
                      cd_debut, "| 키: ", cd_height, "| 몸무게:", cd_weight)

                if gender == '' or cd_gender == '' or birth == '' or cd_birth_ == '':
                    ratio = SequenceMatcher(None, debut, cd_debut).ratio()

                    # 데뷔에 빈 데이터가 있으면 -> 다음 단계로(경력)
                    if debut == '' or cd_debut == '':

                        # 경력사항에 빈 데이터 있으면 -> 다음 단계로(학력)
                        if career == '' or cd_career == '':

                            # 학력에 빈 데이터 있으면 -> 다음 단계로(수상)
                            if academic == '' or cd_academic == '':

                                # 수상내역에 빈 데이터 있으면 -> 다음 단계로 (키&몸무게)
                                if awards == '' or cd_award == '':

                                    # 키 & 몸무게 중 빈 데이터가 있으면
                                    if height == '' and weight == '':
                                        checking.append(peopleCode)
                                        print("[INFO] 비교 데이터 부족! ")

                                    elif cd_height == '' and cd_weight == '':
                                        checking.append(peopleCode)
                                        print("[INFO] 비교 데이터 부족! ")

                                    elif height == '' or cd_height == '' or weight == '' or cd_weight == '':
                                        checking.append(peopleCode)
                                        print("[INFO] 비교 데이터 부족! ")

                                    # 키 & 몸무게 둘 다 데이터 있으면
                                    elif height != '' and cd_height != '' and weight != '' and cd_weight != '':
                                        if abs(float(height) - float(cd_height)) <= 2 and abs(
                                                float(weight) - float(cd_weight)) <= 2:
                                            mapping_data = {'kobis_people_id': peopleCode, 'celeb_idx': int(series_id),
                                                            'cd_name': cd_name}
                                            mapping_2.append(mapping_data)
                                            print("[INFO] height & weight -> 매핑 성공! ")
                                            raise NotImplementedError

                                        else:
                                            newPeople_2.append(peopleCode)
                                            print("[INFO] height & weight -> 매핑 실패! ")
                                            raise NotImplementedError

                                else:
                                    for s in awards.split('|'):
                                        awards_ratio = SequenceMatcher(None, s, cd_award).ratio()
                                        if awards_ratio >= 0.7:
                                            mapping_data = {'kobis_people_id': peopleCode, 'celeb_idx': int(series_id),
                                                            'cd_name': cd_name}
                                            mapping_2.append(mapping_data)
                                            print(s, " | ", cd_award, " | ", awards_ratio)
                                            print("[INFO] awards -> 매핑 성공! ")
                                            raise NotImplementedError
                                        else:
                                            newPeople_2.append(peopleCode)
                                            print(s, " | ", cd_award, " | ", awards_ratio)
                                            print("[INFO] awards -> 매핑 실패!")
                                            raise NotImplementedError

                            else:
                                for s in academic.split('|'):
                                    academic_ratio = SequenceMatcher(None, s, cd_academic).ratio()
                                    if academic_ratio >= 0.7:
                                        mapping_data = {'kobis_people_id': peopleCode, 'celeb_idx': int(series_id),
                                                        'cd_name': cd_name}
                                        mapping_2.append(mapping_data)
                                        print(s, " | ", cd_academic, " | ", academic_ratio)
                                        print("[INFO] academic -> 매핑 성공! ")
                                        raise NotImplementedError
                                    else:
                                        newPeople_2.append(peopleCode)
                                        print(s, " | ", cd_academic, " | ", academic_ratio)
                                        print("[INFO] academic -> 매핑 실패")
                                        raise NotImplementedError


                        # 경력사항에 빈 데이터 없으면
                        else:
                            for s in career.split('|'):
                                career_ratio = SequenceMatcher(None, s, cd_career).ratio()
                                if career_ratio >= 0.7:
                                    mapping_data = {'kobis_people_id': peopleCode, 'celeb_idx': int(series_id),
                                                    'cd_name': cd_name}
                                    mapping_2.append(mapping_data)
                                    print(s, " | ", cd_career, " | ", career_ratio)
                                    print("[INFO] career -> 매핑 성공! ")
                                    raise NotImplementedError

                                else:
                                    newPeople_2.append(peopleCode)
                                    print(s, " | ", cd_career, " | ", career_ratio)
                                    print("[INFO] career -> 매핑 실패")
                                    raise NotImplementedError


                    # 데뷔 이력이 80% 이상 비슷하다면 -> 매핑
                    elif ratio >= 0.8:
                        mapping_data = {'kobis_people_id': peopleCode, 'celeb_idx': int(series_id), 'cd_name': cd_name}
                        mapping_2.append(mapping_data)
                        print("[INFO] debut -> 매핑 성공! ")
                        raise NotImplementedError

                    # 데뷔 이력이 80% 이하 비슷하지 않다면 -> 인물등록
                    elif ratio < 0.8:
                        newPeople_2.append(peopleCode)
                        print("[INFO] debut -> 매핑 실패! ")
                        raise NotImplementedError

                # 성별과 생일 모두 같으면 -> 매핑
                elif (gender == cd_gender) and (birth == cd_birth_):
                    mapping_data = {'kobis_people_id': peopleCode, 'celeb_idx': int(series_id), 'cd_name': cd_name}
                    mapping_2.append(mapping_data)
                    print("[INFO] gender & birth -> 매핑 성공! ")
                    raise NotImplementedError

                    # 성별과 생일 둘 중에 하나라도 다른 값이 있으면 -> 인물등록
                elif (gender != cd_gender) or (birth != cd_birth_):
                    newPeople_2.append(peopleCode)
                    print("[INFO] gender & birth -> 매핑 실패! ")
                    raise NotImplementedError

        except:
            pass

        print("\n")

    mapping_2_df = pd.DataFrame(mapping_2, columns=['kobis_people_id', 'celeb_idx', 'cd_name'])
    mapping_2_list = mapping_2_df['kobis_people_id'].to_list()

    # 수기 매핑 개체들 중복 제거
    checking = list(set(checking))

    for i in checking:
        if i not in newPeople_2:
            checking_.append(i)

    for j in checking_:
        if j not in mapping_2_list:
            checking__.append(j)

    print("[INFO] 2차 매핑 완료 ... \n")

    print('*{0}명 중 {1}명 매핑 성공하였습니다.'.format(len(empty_frame), len(mapping_2_df)))
    print('*{0}명 중 {1}명 매핑 실패하였습니다. 인물등록을 진행하세요.'.format(len(empty_frame), len(newPeople_2)))
    print('*{0}명 중 {1}명 매핑 실패하였습니다. 수기 매핑을 진행하세요.\n'.format(len(empty_frame), len(checking__)))
    return mapping_2_df, newPeople_2, checking__


def db_update_2(mapping_2_df) :

    conn, engine = db_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    duplicated_list2 = []

    # 맵핑된 개체들 movie_people_connection db insert
    if len(mapping_2_df) > 0 :
        # mapping_2_df = mapping_2_df.astype({'kobis_people_id': 'int'})
        # mapping_2_df['update_date'] = data_date
        # mapping_2_df.to_sql('movie_people_connection', engine, if_exists='append', index=None)
        # print("[INFO] 2차 매핑 결과 -> movie_people_connection db insert 완료")

        # 중복 celeb_idx 있는 경우 connection에 insert하지 않음
        for i, row in mapping_2_df.iterrows():
            kobis_people_id = row['kobis_people_id']
            celeb_idx = row['celeb_idx']
            cd_name = row['cd_name']

            qry1 = f'select * from kuk.movie_people_connection where celeb_idx={celeb_idx}'
            result1 = pd.read_sql(qry1, conn)

            if len(result1) == 0:
                qry2 = f'INSERT INTO kuk.movie_people_connection (kobis_people_id, celeb_idx, cd_name, update_date) values ({kobis_people_id}, {celeb_idx}, \'{cd_name}\', NOW())'
                cursor.execute(qry2)
            else:
                duplicated2 = {'kobis_people_id': kobis_people_id, 'celeb_idx': celeb_idx, 'cd_name': cd_name}
                duplicated_list2.append(duplicated2)
                print('! 중복된 celeb_idx 가 있습니다. kobis_people_id: {0}, celeb_idx: {1}, cd_name: {2}'.format(kobis_people_id, celeb_idx, cd_name))
        conn.commit()
        print("[INFO] 2차 매핑 결과 -> movie_people_connection db insert 완료")

        # mapping 여부 db update
        mapping_list = mapping_2_df['kobis_people_id'].to_list()
        mapping_query = "UPDATE maimovie_kr.kobis_people_list SET mapping=1 WHERE peopleCd=%s;"
        cursor.executemany(mapping_query, mapping_list)
        conn.commit()
        print("[INFO] 2차 매핑 결과 -> mapping = 1 db update 완료")

    if len(newPeople_2) > 0 :
        not_mapping_list = newPeople_2
        not_mapping_query = "UPDATE maimovie_kr.kobis_people_list SET mapping=0 WHERE peopleCd=%s;"
        cursor.executemany(not_mapping_query, not_mapping_list)
        conn.commit()
        print("[INFO] 2차 매핑 결과 -> mapping = 0 db update 완료")

    if len(checking__) > 0 :
        checking_list = checking__
        checking_mapping_query = "UPDATE maimovie_kr.kobis_people_list SET mapping=2 WHERE peopleCd=%s;"
        cursor.executemany(checking_mapping_query, checking_list)
        conn.commit()
        print("[INFO] 2차 매핑 결과 -> mapping = 2 db update 완료\n")

    conn.close()

    return duplicated_list2


def report() :

    conn, engine = db_connection()

    query1 = "select COUNT(*) from kuk.movie_people_connection"
    cnt = pd.read_sql(query1, con=conn)
    cnt = cnt.iloc[0, 0]

    query2 = "select * from maimovie_kr.kobis_people_list where mapping=0"
    cnt2 = pd.read_sql(query2, con=conn)

    query3 = "select * from maimovie_kr.kobis_people_list where profileImage <> '' and profileImage is not null and mapping=0"
    cnt3 = pd.read_sql(query3, con=conn)

    query4 = "select * from maimovie_kr.kobis_people_list where mapping=2"
    cnt4 = pd.read_sql(query4, con=conn)


    print('* KOBIS 영화 인물 자동 매핑 결과입니다.')
    print('- DB명 : kuk > movie_people_connection')
    print("매핑 성공 개수 :", len(mapping_1_df) + len(mapping_2_df))
    print("누적 매핑 개수 : {0} / {1}".format(cnt, len(total_kobis)))
    print("인물 등록 대상 : {0}({1})".format(len(cnt2), len(cnt3)))
    print("수기 매핑 대상 : {}".format(len(cnt4)))
    print("총 인물 등록 대상은 {}명, 그 중 네이버 프로필 이미지가 존재하는 개체 수는 {}명으로 {}명을 우선하여 인물 등록 진행하겠습니다.".format(len(cnt2), len(cnt3), len(cnt3)))

    conn.close()


if __name__ == '__main__':
    t = datetime.datetime.now()
    data_date = t.strftime('%Y-%m-%d %H:%M:00')

    total_kobis, kobis_mapping, kobis_not_mapping, kobis_list = getPeople()
    celeb = getCelebPeople()

    # 1. 코비스에 생일 데이터 유무로 데이터 먼저 나누고, 생일 있는 개체들끼리 1차 매핑 / 생일 없는 개체들은 2차 매핑
    birthExist, birthNonExist = get_mapping_data()

    # 2-1. 1차 매핑
    mapping_1_df, newPeople_1, checking__1 = mapping_1()
    # 2-2. 1차 매핑 결과 db 업로드
    duplicated_list1 = db_update_1(mapping_1_df)

    # 2. 생일 없는 개체들 네이버에서 정보 크롤링 후, celeb과 비교하여 2차 매핑
    number = int(input("크롤링할 개체 수 입력 : "))
    empty_frame = naver_crawling(number)

    # 2-1. 2차 매핑
    mapping_2_df, newPeople_2, checking__ = mapping_2()
    # 2-2. 2차 매핑 결과 db 업로드
    duplicated_list2 = db_update_2(mapping_2_df)

    # 3. 보고
    report()

    # 4. 중복 데이터 확인
    timelabel = datetime.strftime(datetime.now(), format='%m%d_%H%M')

    duplicated_list = duplicated_list1 + duplicated_list2
    duplicated = pd.DataFrame(duplicated_list)
    duplicated.to_excel('/Users/jieun/Desktop/업무/인물매핑/KOBIS/중복 데이터/duplicated_data_'+timelabel+'.xlsx')


