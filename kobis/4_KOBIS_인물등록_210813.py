'''

목표 : 신규 인물 등록 대상들을 몽땅 등록 요청 하자 !
대상 : 코비스엔 있으나 celeb엔 없는 인물 중 매핑 기준에 맞는 인물들, 즉 mapping=0 이고, 네이버 프로필에 이미지가 있는 인물등
    (네이버 프로필에 이미지가 없으면 하나하나 다 찾아야 하는데 너무 오래 걸림!!! 일단은 이미지가 있는 사람들만 인물등록 진행)

프로세스 :
-> 대상들을 불러오고
-> 인물 등록 요청 시트에 맞게 데이터프레임 생성 후, 엑셀로 내보내기
-> 후에 인물 등록이 완료 되면 series_id를 엑셀에 붙여넣고,
-> 불러와서 db 업데이트 진행 (mapping=0 -> mapping=1 로 업데이트 & kuk.movie_people_connection 에 insert)

* 참고
코드상 오류가 (ㅠㅠ) 있을 수 있기 때문에 등록 요청을 하면서 꼭! celeb 에 있는지 없는지 확인해봐야 함ㅎㅎ.. <중복 검수>

1. celeb 에 이미 등록이 돼있으면
    -> maimovie_kr.kobis_people_list > 해당 peopleCd 찾아서 mapping=1 로 수정
    -> kuk.movie_people_connection 에 insert
        INSERT into movie_people_connection (kobis_people_id, celeb_idx, cd_name, update_date) values ({peopleCd}, {series_id}, {name}, NOW())
        INSERT into movie_people_connection values (, , '', NOW())
    -> 엑셀에서도 지우기~!



2. celeb 에 이미 등록이 돼있는데 매핑까지 돼있는 경우 = 동일 인물인데 peopleCd가 다른 경우.... (코비스 문제임)
    -> 코비스에 들어가서 진짜로 중복으로 등록돼있는지 확인하고
    -> 두 개의 peopleCd 중 아무거나.. 정보가 좀 더 많은 쪽으로 매핑해주면 됨
    -> 두 개의 peopleCd 다 mapping=1로 수정하고
    -> movie_people_connection 엔 peopleCd 하나만!
    -> 이 경우(peopleCd가 코비스에 중복으로 등록돼있는 경우)는 논의 후에 방법이 바뀔 수 있음

'''
# import os
# os.environ["MKL_NUM_THREADS"] = "1"
# os.environ["NUMEXPR_NUM_THREADS"] = "1"
# os.environ["OMP_NUM_THREADS"] = "1"

import pandas as pd
import numpy as np
# import datetime
from datetime import datetime
import pymysql
from sqlalchemy import create_engine



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



# 인물등록할 인물들 데이터 프레임 가져오기
def produce_dataframe(num):
    conn, engine = db_connection()

    query = "select * from maimovie_kr.kobis_people_list where profileImage <> '' and profileImage is not null and mapping=0"
    data = pd.read_sql(query, conn)
    data = data.iloc[:num, :]

    conn.close()

    return data


def produce_dataframe2(num, name, category):
    empty_frame = pd.DataFrame(
        columns=("peopleCd", "profileImage", "날짜", "요청자", "카테고리", "이름", "daum_url", "naver_url", "cd_idx"), index=np.arange(num))
    empty_frame["peopleCd"] = list(data.peopleCd)
    empty_frame["profileImage"] = list(data.profileImage)
    empty_frame["날짜"] = date
    empty_frame["요청자"] = name
    empty_frame["카테고리"] = category
    empty_frame["이름"] = list(data.peopleNm)
    empty_frame["daum_url"] = 'x'
    empty_frame["naver_url"] = list(data.profile_naverURL)

    print("인물등록을 진행하세요.")
    return empty_frame




def db_update(after) :
    conn, engine = db_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    duplicated_list = []

    after = after[['peopleCd', 'cd_idx', '이름']]
    after = after.rename({'peopleCd': 'kobis_people_id', 'cd_idx' : 'celeb_idx', '이름' : 'cd_name'}, axis=1)
    # after = after.astype({'celeb_idx': 'int'})
    # after['update_date'] = data_date
    # after.to_sql('movie_people_connection', engine, if_exists='append', index=None)
    # print("[INFO] 인물등록 결과 -> movie_people_connection db insert 완료")

    for i, row in after.iterrows():
        kobis_people_id = row['kobis_people_id']
        celeb_idx = row['celeb_idx']
        cd_name = row['cd_name']

        qry1 = f'select * from kuk.movie_people_connection where celeb_idx={celeb_idx}'
        result1 = pd.read_sql(qry1, conn)

        if len(result1) == 0:
            qry2 = f'INSERT INTO kuk.movie_people_connection (kobis_people_id, celeb_idx, cd_name, update_date) values ({kobis_people_id}, {celeb_idx}, \'{cd_name}\', NOW())'
            cursor.execute(qry2)
        else:
            duplicated = {'kobis_people_id': kobis_people_id, 'celeb_idx': celeb_idx, 'cd_name': cd_name}
            duplicated_list.append(duplicated)
            print('! 중복된 celeb_idx 가 있습니다. kobis_people_id: {0}, celeb_idx: {1}, cd_name: {2}'.format(kobis_people_id,
                                                                                                      celeb_idx,
                                                                                                      cd_name))
    conn.commit()
    print("[INFO] 인물등록 결과 -> movie_people_connection db insert 완료")

    # mapping 여부 db update
    mapping_list = after['kobis_people_id'].to_list()
    mapping_query = "UPDATE maimovie_kr.kobis_people_list SET mapping=1 WHERE peopleCd=%s;"
    cursor.executemany(mapping_query, mapping_list)
    conn.commit()
    print("[INFO] 인물등록 결과 -> mapping = 1 db update 완료")

    return duplicated_list


def newPeople() :
    ans = input('인물등록이 완료되었나요?(y/n)')
    if ans.lower() == 'y' :
        duplicated_list = db_update(after)
        return duplicated_list

    else :
        return


def getPeople():
    conn, engine = db_connection()
    query = "select * from maimovie_kr.kobis_people_list"
    kobis = pd.read_sql(query, con=conn)

    kobis = kobis[
        (kobis['repRoleNm'] == '배우') | (kobis['repRoleNm'] == '감독') | (kobis['repRoleNm'] == '조감독')]  # 배우, 감독, 조감독인 사람들
    kobis = kobis.drop_duplicates(subset='peopleCd', keep='first')  # 중복 제거
    kobis = kobis.dropna(subset=['profile_naverURL'])  # 네이버 url 있는 사람들

    # 이름 글자 수가 5이상이면 외국인이라고 판단....?
    kobis['nm_num'] = kobis.peopleNm.str.len()  # 이름 글자 수 추출
    kobis = kobis[kobis['nm_num'] <= 4]  # 한국인만 추출

    total_kobis = kobis  # total 매핑 대상

    return total_kobis


def report() :
    conn, engine = db_connection()
    total = getPeople()

    query = "select COUNT(*) from kuk.movie_people_connection"
    cnt = pd.read_sql(query, con=conn)
    cnt = cnt.iloc[0, 0]

    query2 = "select * from maimovie_kr.kobis_people_list where profileImage <> '' and profileImage is not null and mapping=0"
    after_cnt = pd.read_sql(query2, con=conn)

    print('* KOBIS 영화 인물 등록 현황입니다.')
    print('- DB명 : kuk > movie_people_connection')
    print("금일 등록 개수 :", len(after))
    # print("누적 등록 개수 :(누적 + 금일) / ({} + (누적 + 금일))".format(len(after_cnt)))
    # print("누적 등록 개수 :(누적 + 금일) / {}".format(len(after_cnt) + len(after)))
    print("누적 등록 개수 : (누적 + 금일) ")
    print("남은 등록 개수 :", len(after_cnt))
    print("누적 매핑 개수 : {0} / {1}".format(cnt, len(total)))
    conn.close()



if __name__ == '__main__':

    now = datetime.now()
    date = now.strftime('%Y-%m-%d')
    date_ = now.strftime('%y%m%d')
    data_date = now.strftime('%Y-%m-%d %H:%M:00')

    path = '/Users/jieun/Desktop/업무/인물매핑/KOBIS/인물등록/'

    num = int(input("인물등록 할 사람 수 :"))
    data = produce_dataframe(num)
    data2 = produce_dataframe2(num, "이름", "코비스")
    data2.to_excel(path + date + '_코비스_인물등록.xlsx')


    ###################################################################
    # 인물등록 후, 엑셀 파일에 cd_idx 붙여넣고 load
    after = pd.read_excel(path + date + '_코비스_인물등록.xlsx', index_col=0)
    # after = pd.read_excel('/Users/jieun/Desktop/업무/인물매핑/KOBIS/인물등록/2021-08-02_코비스_인물등록.xlsx', index_col=0)
    # db 업로드 진행
    duplicated_list = newPeople()

    # 보고
    report()

    # 중복 데이터
    timelabel = datetime.strftime(datetime.now(), format='%m%d_%H%M')

    duplicated = pd.DataFrame(duplicated_list)
    duplicated.to_excel('/Users/jieun/Desktop/업무/인물매핑/KOBIS/중복 데이터/duplicated_data_등록'+timelabel+'.xlsx')
