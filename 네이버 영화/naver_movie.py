import pandas as pd
import pymysql
import datetime

def db_connection(host_name='ds'):
    host_url = "db.ds.mycelebs.com"
    user_nm =
    passwd =
    port_num = 3306
#     db_name = "star_ko"
    conn = pymysql.connect(host=host_url, user=user_nm, passwd=passwd, port = port_num, charset='utf8',
                           cursorclass=pymysql.cursors.DictCursor)
    # cursorclass=pymysql.cursors.DictCursor 추가 -> 데이터프레임으로 다루기 쉽게 딕셔너리 형태로 결과 반환해줌, cursor는 튜플형태
    # db=db,
    return conn


def getProfilePeople() :
    conn = db_connection()

    qry = "SELECT * FROM star_ko.star_ko_profile_url"
    data = pd.read_sql(qry, conn)

    data = data.dropna(subset=['naver_people_id'])  # null 제거
    data_list = data['naver_people_id'].to_list()
    data_list = list(map(int, data_list))
    data_list = list(map(str, data_list))

    conn.close()

    return data_list


def getMaimoviePeople(who) :
    conn = db_connection()

    if who == '배우' :
        qry = "SELECT * FROM maimovie_kr.maimovie_kr_naver_cast_year WHERE n_people_code not in (124232, 14071782, 14085252, 109222) "
        naver = pd.read_sql(qry, conn)

        naver = naver.drop_duplicates(subset='n_people_code', keep='first')  # 중복 제거
        naver = naver.dropna(subset=['n_people_code'])  # null 제거

        total = naver

        # 한국인만
        naver = naver[naver['n_actor_name_en'].isnull()]  # 영어 이름이 없고

        contains_blank = naver['n_actor_name'].str.contains(" ")
        naver = naver[~contains_blank]  # 이름에 공백이 없고

        naver['nm_num'] = naver.n_actor_name.str.len()
        naver = naver[naver['nm_num'] <= 4]  # 이름 총 글자 수가 4개 이하인 개체

        naver = naver[naver['n_people_code'].isin(data_list) == False] # star_ko_profile_url에 없는 인물

        naver = naver[['pk', 'n_actor_name', 'n_actor_moviecode', 'n_people_code']]
        naver.columns = ['pk', 'name', 'naver_movie_people_id', 'naver_id']


    elif who == '감독':
        qry = "SELECT * FROM maimovie_kr.maimovie_kr_naver_crew_year WHERE n_people_code not in (124232, 14071782, 14085252, 109222)"
        naver = pd.read_sql(qry, conn)

        naver = naver.drop_duplicates(subset='n_people_code', keep='first')  # 중복 제거
        naver = naver.dropna(subset=['n_people_code'])  # null 제거

        total = naver

        # 한국인만
        naver = naver[naver['n_director_name_en'].isnull()]  # 영어 이름이 없고

        contains_blank = naver['n_director_name'].str.contains(" ")
        naver = naver[~contains_blank]  # 이름에 공백이 없고

        naver['nm_num'] = naver.n_director_name.str.len()
        naver = naver[naver['nm_num'] <= 4]  # 이름 총 글자 수가 4개 이하인 개체

        naver = naver[naver['n_people_code'].isin(data_list) == False] # star_ko_profile_url에 없는 인물

        naver = naver[['pk', 'n_director_name', 'n_director_moviecode', 'n_people_code']]
        naver.columns = ['pk', 'name', 'naver_movie_people_id', 'naver_id']

    conn.close()

    return naver, total


def produce_dataframe() :
    naver_cast_ex = naver_cast[:int(num / 2)]
    naver_crew_ex = naver_crew[:int(num / 2)]

    naver_ex = pd.concat([naver_cast_ex, naver_crew_ex])

    naver_ex['date'] = date
    naver_ex['요청자'] = '박병주'
    naver_ex['카테고리'] = '영화'
    naver_ex['daum_url'] = 'x'
    naver_ex['naver_url'] = 'https://search.naver.com/search.naver?where=nexearch&sm=tab_etc&mra=bjky&pkid=1&os=' + \
                             naver_ex['naver_id'] + '&qvt=0&query=' + naver_ex['name']

    naver = naver_ex[['naver_movie_people_id', 'date', '요청자', '카테고리', 'name', 'daum_url', 'naver_url']]


    return naver




if __name__ == '__main__':
    now = datetime.datetime.now()
    date = now.strftime('%Y-%m-%d')

    path = '/Users/jieun/Desktop/업무/인물매핑/naver_movie/'

    data_list = getProfilePeople()

    naver_cast, total_cast = getMaimoviePeople('배우')
    naver_crew, total_crew = getMaimoviePeople('감독')

    num = int(input('등록할 영화 인물 수 :'))
    naver = produce_dataframe()
    naver.to_excel(path + '인물등록/' + date + '_영화_인물등록.xlsx', index=False)