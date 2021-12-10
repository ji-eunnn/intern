import os
import pandas as pd
import re
from datetime import datetime
import pymysql

# db connect 함수
def db_connection(host_name='ds'):
    host_url = "db.ds.mycelebs.com"
    user_nm =
    passwd =
    port_num = 3306
    db_name = "star_ko"
    conn = pymysql.connect(host=host_url, user=user_nm, passwd=passwd, port = port_num, charset='utf8',
                           db = db_name,cursorclass=pymysql.cursors.DictCursor)
    # cursorclass=pymysql.cursors.DictCursor 추가 -> 데이터프레임으로 다루기 쉽게 딕셔너리 형태로 결과 반환해줌, cursor는 튜플형태
    # db=db,
    return conn


def get_datetime():
    now_time = datetime.now()
    now_str = datetime.strftime(now_time, "%Y-%m-%d")
    return now_str


def schema_for_star_data_members():
    sql_insert_star_ko_members = '''INSERT INTO `star_ko_members` (`group_id`, `group_name`, `related_series_id`, `related_name`, `group_type`, `gender`) VALUES({group_id}, '{group_name}', {related_series_id}, '{related_name}', '{group_type}', '{gender}');
    '''
    return sql_insert_star_ko_members


def rearrange_folder_files(path):
    if not os.path.isdir(path):
        os.mkdir(path)
    if os.path.exists(path + '/insert_star_ko_members.sql'):
        os.remove(path + '/insert_star_ko_members.sql')


def define_save_path(datetime):
    save_p = '/Users/jieun/Desktop/program/1_update_data/result/' + datetime
    return save_p


def create_star_data_members(data_frame, path, query_template):
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    for idx, row in data_frame.iterrows():
        if row['group_id'] == row['related_series_id']:
            group_type = 'member'
        else:
            group_type = 'solo'

        if '남' in str(row['gender']):
            gender = '남'
        elif '여' in str(row['gender']):
            gender = '여'
        else:
            gender = ''

        group_id = row['group_id']
        group_name = row['group_name']
        related_series_id = str(row['related_series_id'])
        related_name = re.sub(re.compile('\(.*?\)'), "", row['related_name'])
        group_type = group_type
        gender = gender

        query = f'INSERT INTO `star_ko_members` (`group_id`, `group_name`, `related_series_id`, `related_name`, `group_type`, `gender`) VALUES({group_id}, \'{group_name}\', {related_series_id}, \'{related_name}\', \'{group_type}\', \'{gender}\');'
        cursor.execute(query)
        conn.commit()
    print("[INFO] 신규 인물 star_ko_members 업데이트 완료!")

    #     with open(path + '/insert_star_ko_members.sql', 'at') as f:
    #         f.write(query_template.format(
    #             group_id=row['group_id'], group_name=row['group_name'],
    #             # related_series_id=row['related_series_id'], related_name=row['related_name'],
    #             related_series_id=str(row['related_series_id']), related_name=re.sub(re.compile('\(.*?\)'), "", row['related_name']),
    #             group_type=group_type, gender=gender))
    # print('[INFO] 멤버 파일(star_ko_members)을 저장했습니다.')



if __name__ == '__main__':
    conn = db_connection()
    now_datetime = get_datetime()
    # df_star_ko_members = pd.read_excel(f'/Users/jaemoahh/Desktop/program/1_update_data/result/2020-01-07/star_ko_members.xlsx')
    df_star_ko_members = pd.read_excel(f'/Users/jieun/Desktop/program/1_update_data/result/{now_datetime}/star_ko_members.xlsx')
    df_star_ko_members = df_star_ko_members.fillna('NULL')  # 빈값을 null로 대체 (19/09/04 수정)

    save_dir = define_save_path(now_datetime)
    rearrange_folder_files(save_dir)

    sql_insert_query_star_ko_members = schema_for_star_data_members()

    create_star_data_members(df_star_ko_members, save_dir, sql_insert_query_star_ko_members)
