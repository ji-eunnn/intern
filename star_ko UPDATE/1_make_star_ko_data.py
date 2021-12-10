import pandas as pd
import datetime
import tqdm
import os
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
    now_time = datetime.datetime.now()
    now_str = datetime.datetime.strftime(now_time, "%Y-%m-%d")
    return now_str

def define_save_path(datetime):
    save_p = '/Users/jieun/Desktop/program/1_update_data/result/' + datetime
    return save_p

def rearrange_folder_files(path):
    if not os.path.isdir(path):
        os.mkdir(path)
    if os.path.exists(path + '/insert_star_ko_data.sql'):
        os.remove(path + '/insert_star_ko_data.sql')
    if os.path.exists(path + '/insert_star_ko_special_day.sql'):
        os.remove(path + '/insert_star_ko_special_day.sql')

# db에 접속해서 마지막으로 업데이트한 series_id 값 불러오기
def check_last_updated_celeb_from_db() :
    last_update_celeb = pd.read_sql('''
        SELECT max(series_id) FROM star_ko_data
        UNION ALL
        SELECT max(series_id) FROM star_ko_special_day
        UNION ALL
        SELECT max(group_id) FROM star_ko_members
        ;
        ''',con=conn)
    return last_update_celeb

def schema_for_special_day():
    sql_insert_special_day = '''INSERT INTO `star_ko_special_day` (`series_id`, `content_idx`, `date`, `regist_date`) VALUES ({series_id}, 1, '{cd_birth}', NOW());
    '''
    return sql_insert_special_day

# 업데이트할 신규 데이터 불러오기
def get_new_celebs_for_star_data(last_update_celeb):
    li = list(last_update_celeb['max(series_id)'])
    li_star = str(li[0])
    li_special_day = str(li[1] + 1)
    li_member = str(li[2] + 1)

    sql_special_day = f"SELECT * FROM star_ko_data WHERE series_id BETWEEN {li_special_day} AND {li_star}"
    sql_member = f"SELECT * FROM star_ko_data WHERE series_id BETWEEN {li_member} AND {li_star}"

    results_special_day = pd.read_sql(sql_special_day, con=conn)
    results_member = pd.read_sql(sql_member, con=conn)
    return results_special_day, results_member

# 생일 데이터 만들고, db에 insert
def create_special_day_data(star_ko_special_day, sql_insert_special_day):
    data = []
    series_id = star_ko_special_day['series_id']
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    for i in tqdm.tqdm(series_id):
        v = star_ko_special_day[star_ko_special_day['series_id'] == i]['cd_birth'].tolist()
        if isinstance(v, list) and v[0] != '0000-00-00 00:00:00':
            try :
                birth_day = v[0].strftime('%Y.%m.%d')
                data_ = {'series_id': i, 'cd_birth': birth_day}
                data.append(data_)
            except :
                pass

    if not data :
        print("[INFO] 금일자 insert하는 신규 star_ko_special_day는 없습니다.")
    else :
        for i in range(len(data)):
            idx = str(data[i]['series_id'])
            cd_birth = str(data[i]['cd_birth'])

            query = 'INSERT INTO `star_ko_special_day` (`series_id`, `content_idx`, `date`, `regist_date`) VALUES (' + idx + ', 1, \'' + cd_birth + ' 00:00:00\', NOW()); \n'
            cursor.execute(query)
            conn.commit()
            # print(idx)
        print("[INFO] 신규 인물 star_ko_special_day 업데이트 완료!")

# 멤버 데이터 엑셀로 만들기
def create_member_data_to_excel():
    df_star_ko_members = pd.DataFrame(
        columns=['group_id', 'group_name', 'related_series_id', 'related_name', 'group_type', 'gender'])

    bin = []
    for i in star_ko_members.index:
        # global li_members
        if star_ko_members.loc[i, 'cd_group_flag'] == 1 and star_ko_members.loc[i, 'cd_members'] != None:
            li_members = star_ko_members.loc[i, 'cd_members'].split(',')
            bin.append(li_members)
            # print(li_members)

            df_star_ko_members.loc[len(df_star_ko_members)] = {
                'group_id': star_ko_members.loc[i, 'series_id'], 'group_name': star_ko_members.loc[i, 'cd_name'],
                'related_series_id': star_ko_members.loc[i, 'series_id'], 'related_name': star_ko_members.loc[i, 'cd_name'],
                'group_type': 'group', 'gender': ''}

            for member in li_members:
                df_star_ko_members.loc[len(df_star_ko_members)] = {
                    'group_id': star_ko_members.loc[i, 'series_id'], 'group_name': star_ko_members.loc[i, 'cd_name'],
                    'related_series_id': '', 'related_name': member.strip(), 'group_type': 'solo', 'gender': ''}

        else :

            pass

    if len(bin) >= 1 :
        rearrange_folder_files(save_dir)
        df_star_ko_members.to_excel(save_dir + '/star_ko_members.xlsx', index=False)
        print("[INFO] 업데이트할 신규 인물 star_ko_members 있음")
    else :
        print("[INFO] 금일자 insert하는 신규 star_ko_members 없습니다.")







if __name__ == '__main__':

    now_datetime = get_datetime()
    save_dir = define_save_path(now_datetime)
    # rearrange_folder_files(save_dir)

    conn = db_connection()

    last_update_celeb = check_last_updated_celeb_from_db()
    star_ko_special_day, star_ko_members = get_new_celebs_for_star_data(last_update_celeb)

    sql_insert_special_day = schema_for_special_day()

    create_special_day_data(star_ko_special_day, sql_insert_special_day)
    create_member_data_to_excel()

    conn.close()



