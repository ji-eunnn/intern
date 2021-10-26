from tqdm import tqdm
import re
import pymysql
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime

def db_connection(host_name='ds'):
    host_url = "db.ds.mycelebs.com"
    user_nm = "j_eungg"
    passwd = "wldms10529"
    port_num = 3306
    db_name = "star_ko"
    conn = pymysql.connect(host=host_url, user=user_nm, passwd=passwd, port = port_num, charset='utf8',
                           db = db_name,cursorclass=pymysql.cursors.DictCursor)
    # cursorclass=pymysql.cursors.DictCursor 추가 -> 데이터프레임으로 다루기 쉽게 딕셔너리 형태로 결과 반환해줌, cursor는 튜플형태
    # db=db,
    engine = create_engine(f'mysql+pymysql://{user_nm}:{passwd}@{host_url}:{port_num}/{db_name}?charset=utf8mb4')
    return conn, engine


def produce_dataframe(df) :
    df = df.fillna('x')
    df = df.replace('X', 'x')
    df = df.replace('_x0008_X', 'x')

    df['daum_people_id'] = ''
    df['naver_people_id'] = ''

    for i, row in tqdm(df.iterrows(), total=df.shape[0]):
        name = row['name']
        daum = row['daum_url']
        naver = row['naver_url']
        series_id = int(row['series_id'])

        try:
            daum_id = re.findall('view/(\S+)', daum)[0]
        except:
            daum_id = None
        try:
            naver_id = re.findall('&os=(\d+)', naver)[0]
        except:
            naver_id = None

        df['daum_people_id'][i] = daum_id
        df['naver_people_id'][i] = naver_id

    df['regist_date'] = date
    df['update_date'] = None

    df = df[['series_id', 'name', 'daum_url', 'naver_url', 'daum_people_id', 'naver_people_id', 'regist_date',
             'update_date']]

    return df


if __name__ == '__main__':

    now = datetime.now()
    date = now.strftime('%Y-%m-%d %H:%m:%S')

    df = pd.read_excel('/Users/jieun/Desktop/업무/인물등록/인물카운트.xlsx')
    df = produce_dataframe(df)

    conn, engine = db_connection()
    engine_conn = engine.connect()
    df.to_sql("star_ko_profile_url", engine_conn, if_exists='append', index=None)

    conn.close()
    engine_conn.close()
    engine.dispose()