import pandas as pd
import pymysql

def count(category) :
    cnt = df[df['카테고리'] == category].count()[0]
    
    return cnt

def db_connection(host_name='ds'):
    host_url = "db.ds.mycelebs.com"
    user_nm = "j_eungg"
    passwd = "wldms10529"
    port_num = 3306
    # db_name = "star_ko"
    conn = pymysql.connect(host=host_url, user=user_nm, passwd=passwd, port = port_num, charset='utf8',
                           cursorclass=pymysql.cursors.DictCursor)
    # cursorclass=pymysql.cursors.DictCursor 추가 -> 데이터프레임으로 다루기 쉽게 딕셔너리 형태로 결과 반환해줌, cursor는 튜플형태
    # db=db,
    return conn


def getTotal() :
    conn = db_connection()

    qry = 'select count(*) from star_ko.star_ko_data where cd_is_use=1'
    data = pd.read_sql(qry, conn)
    total = data.iloc[0, 0]

    conn.close()

    return total


def getMaimoviePeople() :
    conn = db_connection()

    qry1 = "SELECT * FROM maimovie_kr.maimovie_kr_naver_cast_year"
    naver_cast = pd.read_sql(qry1, conn)

    naver_cast = naver_cast.drop_duplicates(subset='n_people_code', keep='first')  # 중복 제거
    naver_cast = naver_cast.dropna(subset=['n_people_code'])  # null 제거


    qry2 = "SELECT * FROM maimovie_kr.maimovie_kr_naver_crew_year"
    naver_crew = pd.read_sql(qry2, conn)

    naver_crew = naver_crew.drop_duplicates(subset='n_director_peoplecode', keep='first')  # 중복 제거
    naver_crew = naver_crew.dropna(subset=['n_director_peoplecode'])  # null 제거

    conn.close()

    return naver_cast, naver_crew


df = pd.read_excel('/Users/jieun/Desktop/업무/인물등록/인물카운트.xlsx')
tv = pd.read_excel('/Users/jieun/Desktop/업무/인물등록/tv_celeb_enroll_final_new.xlsx')
total = getTotal()
naver_cast, naver_crew = getMaimoviePeople()

gaon_cnt = count('youtube') + count('smr') + count('vlive') + count('익디')
all_cnt = gaon_cnt + count('공연') + count('영화') + count('방송') + count('광고') + count('인플루언서') + count('버추얼스타')

print('*** 데일리 인물 등록 및 매핑 현황입니다. @Sanguine Kim @오승훈')
print('- 가온(youtube, smr, vlive) & 익디')
print('금일 등록 개수 :', gaon_cnt)

print('- 공연')
print('금일 등록 개수 :', count('공연'))

print('- kr movie')
print('금일 등록 개수 :', count('영화'))
print('누적 등록 개수 : (누적 + 금일) /', len(naver_cast) + len(naver_crew))

print('- kr movie (코비스)')
print('누적 등록 개수 : 4836')
print('남은 등록 개수 : 1456')
print('누적 매핑 개수 : 6625 / 39380')

print('- kr tv')
print('금일 등록 개수 :', count('방송'))
print('누적 등록 개수 :', tv.cd_idx.count(), '/ 9308')

print('- 광고협회')
print('금일 등록 개수 :', count('광고'))

print('- 인플루언서')
print('금일 등록 개수 :', count('인플루언서'))

print('- 버추얼스타')
print('금일 등록 개수 :', count('버추얼스타'))

print('* 데일리 등록 현황 합계')
print('금일 등록 개수 :', all_cnt)
print('누적 등록 개수 :', all_cnt + total)

print("--------------------------------------------")

print('* 네이버 영화 인물 등록 현황입니다.')
print('금일 등록 개수 :', count('영화'))
print('누적 등록 개수 : (누적 + 금일) /', len(naver_cast) + len(naver_crew))

print('* 코비스 영화 인물 등록 현황입니다.')
print('누적 등록 개수 : 4836')
print('남은 등록 개수 : 1456')
print('누적 매핑 개수 : 6625 / 39380')