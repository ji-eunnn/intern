'''

목표 : 자동 매핑이 안된 개체들을 "수동으로" 매핑하자 !
매핑 대상 : mapping=2인 개체들

매핑 프로세스 :
-> kobis 데이터와 celeb 내 동명이인들 데이터를 출력해서 비교
-> 같은 인물인 경우, input에 series_id를 입력하면 매핑이 되고
-> 같은 인물이 아닌 경우, input에 아무것도 입력 x
-> 매핑이 된 개체들은 mapping=1로 업데이트가 되고, kuk.movie_people_connection 에도 자동으로 insert 됨
-> 매핑이 안된 개체들은 mapping=0로 업데이트 되고, 후에 인물등록 진행하면 됨

* 참고
파이참으로 수기매핑하면 사진이 안떠서, 주피터로 매핑하는게 더 편함..!

'''


import pandas as pd
import pymysql
from sqlalchemy import create_engine
from PIL import Image
import requests
import datetime
from io import BytesIO
import tqdm


def db_connection(host_name='ds'):
    host_url = "db.ds.mycelebs.com"
    user_nm = "j_eungg"
    passwd = "mycelebs"
    port_num = 3306
#     db_name = "maimovie_kr"
    conn = pymysql.connect(host=host_url, user=user_nm, passwd=passwd, port = port_num, charset='utf8',
                           cursorclass=pymysql.cursors.DictCursor)
    # cursorclass=pymysql.cursors.DictCursor 추가 -> 데이터프레임으로 다루기 쉽게 딕셔너리 형태로 결과 반환해줌, cursor는 튜플형태
    # db=db,
    engine = create_engine(f'mysql+pymysql://{user_nm}:{passwd}@{host_url}:{port_num}/kuk?charset=utf8mb4')
    return conn, engine


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

    query_ = "select * from maimovie_kr.kobis_people_list where mapping=2"
    data = pd.read_sql(query_, conn)

    print("[INFO] data load DONE!\n")
    conn.close()

    return total_kobis, data


# 수기 매핑
def handMapping(number) :
    conn, engine = db_connection()

    ex = data.iloc[:number, :]

    idx_ = []
    ex['celeb_idx'] = ''

    for i, row in tqdm(ex.iterrows(), total=ex.shape[0]):

        peopleCode = row['peopleCd']
        name = row['peopleNm']
        gender = row['sex']
        job = row['repRoleNm']
        url = row['profile_naverURL']
        img = row['profileImage']
        url = row['profile_naverURL']

        print("-------- KOBIS 데이터 ----------")
        try:
            kd_response = requests.get(img)
            kd_img = Image.open(BytesIO(kd_response.content))
            print(kd_img)
        except:
            pass

        print("인물코드: ", peopleCode)
        print("이름: ", name, "| 직업: ", job, "| 네이버: ", url)

        query = f'''select *
                from star_ko.star_ko_data
                where (cd_name='{name}' or cd_real_name='{name}')
                    and (cd_category LIKE '%감독%' OR cd_category LIKE '%배우%' OR cd_category LIKE '%방송인%' OR cd_category LIKE '%탤런트%' OR cd_category LIKE '%작가%' OR cd_category LIKE '%PD%' OR cd_category LIKE '%연출%') and cd_is_use=1
                    '''
        celeb = pd.read_sql(query, con=conn)

        for c, crow in celeb.iterrows():
            cd_name = crow['cd_name']
            series_id = crow['series_id']
            cd_category = crow['cd_basic_info']
            cd_gender = crow['cd_gender']
            cd_birth = str(crow['cd_birth'])
            cd_solr_search = crow['cd_solr_search']
            #         cd_agency = crow['cd_agency']
            cd_debut = crow['cd_debut']
            cd_img_url = crow['profile_url_main']


            keywords = ['man', 'woman', 'group']
            result = any(keyword in cd_img_url for keyword in keywords)

            print("---------- 셀럽 데이터 ----------")

            if not result:  # 스타ai에 이미지가 있으면
                print("seires_id: ", series_id)
                try:
                    cd_img_url = crow['profile_url_main']
                    cd_response = requests.get(cd_img_url)
                    cd_img = Image.open(BytesIO(cd_response.content))
                    cd_img = cd_img.resize((120, 150))
                    print(cd_img)
                except:
                    pass

            else:
                pass

            donut = f"http://dev.mycelebs.com/donut/Celeb/ShowManageCeleb/{series_id}"
            print("series_id: ", series_id)
            print("이름: ", cd_name, "| 직업: ", cd_category, "| 생일: ", cd_birth[0:-9], "| 솔라쿼리: ", cd_solr_search,
                  "| 데뷔: ", cd_debut, "| 도넛: ", donut)

            idx = input("series_id를 매핑해주세요 : ")
            idx_.append(idx)

            if idx == '':
                print("[INFO] 매핑 실패!\n")
            else:
                ex['celeb_idx'][i] = idx
                print("[INFO] 매핑 완료!\n")

    conn.close()

    return ex



def db_update(ex) :

    conn, engine = db_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    duplicated_list = []

    ex = ex.rename({'peopleCd': 'kobis_people_id', 'peopleNm': 'cd_name'}, axis=1)

    mapping_data = ex[ex['celeb_idx'] != '']
    newPeople_data = ex[ex['celeb_idx'] == '']

    # 맵핑된 개체들 movie_people_connection db insert
    if len(mapping_data) > 0:
        mapping_df = mapping_data.loc[:, ['kobis_people_id', 'celeb_idx', 'cd_name']]
        # mapping_df = mapping_df.astype({'celeb_idx': 'int'})
        # mapping_df['update_date'] = data_date
        # mapping_df.to_sql('movie_people_connection', engine, if_exists='append', index=None)
        # print("[INFO] 수기 매핑 결과 -> movie_people_connection db insert 완료")

        for i, row in mapping_df.iterrows():
            kobis_people_id = row['kobis_people_id']
            celeb_idx = row['celeb_idx']
            cd_name = row['cd_name']

            qry = f'select * from kuk.movie_people_connection where celeb_idx={celeb_idx}'
            result = pd.read_sql(qry, conn)

            if len(result) == 0:
                qry2 = f'INSERT INTO kuk.movie_people_connection (kobis_people_id, celeb_idx, cd_name, update_date) values ({kobis_people_id}, {celeb_idx}, \'{cd_name}\', NOW())'
                cursor.execute(qry2)
            else:
                duplicated = {'kobis_people_id': kobis_people_id, 'celeb_idx': celeb_idx, 'cd_name': cd_name}
                duplicated_list.append(duplicated)
                print('! 중복된 celeb_idx 가 있습니다. kobis_people_id: {0}, celeb_idx: {1}, cd_name: {2}'.format(kobis_people_id, celeb_idx, cd_name))

        print("[INFO] 1차 매핑 결과 -> movie_people_connection db insert 완료")
        conn.commit()

        # mapping 여부 db update
        mapping_list = mapping_df['kobis_people_id'].to_list()
        mapping_query = "UPDATE maimovie_kr.kobis_people_list SET mapping=1 WHERE peopleCd=%s;"
        cursor.executemany(mapping_query, mapping_list)
        conn.commit()
        print("[INFO] 수기 매핑 결과 -> mapping = 1 db update 완료")

    if len(newPeople_data) > 0:
        not_mapping_list = newPeople_data['kobis_people_id'].to_list()
        not_mapping_query = "UPDATE maimovie_kr.kobis_people_list SET mapping=0 WHERE peopleCd=%s;"
        cursor.executemany(not_mapping_query, not_mapping_list)
        conn.commit()
        print("[INFO] 수기 매핑 결과 -> mapping = 0 db update 완료")

    conn.close()

    return mapping_data, duplicated_list


def report(mapping_data, total_kobis, data) :
    conn, engine = db_connection()

    query = "select COUNT(*) from kuk.movie_people_connection"
    cnt = pd.read_sql(query, con=conn)
    cnt = cnt.iloc[0, 0]

    print('* KOBIS 영화 인물 수기 매핑 결과입니다.')
    print('- DB명 : kuk > kobis_people_connection')
    print("수기 매핑 결과 : {0} / {1}".format(len(mapping_data), len(data)))
    print("누적 매핑 개수 : {0} / {1}".format(cnt, len(total_kobis)))

    conn.close()



if __name__ == '__main__':
    now = datetime.datetime.now()
    data_date = now.strftime('%Y-%m-%d %H:%M:00')

    # 데이터 로드
    total_kobis, data = getPeople()

    # 수기 매핑
    number = int(input("매핑할 개체 수 입력 : "))
    ex = handMapping(number)

    # db 업로드
    mapping_data, duplicated_list = db_update(ex)

    # 보고
    report(mapping_data, total_kobis, data)

    # 중복 데이터
    timelabel = datetime.strftime(datetime.now(), format='%m%d_%H%M')

    duplicated = pd.DataFrame(duplicated_list)
    duplicated.to_excel('/Users/jieun/Desktop/업무/인물매핑/KOBIS/중복 데이터/duplicated_data_수기_'+timelabel+'.xlsx')
