import re
import json
from tqdm import tqdm
import pandas as pd
from solr_query import check_solr_query
from databasehandler import DataBaseHandler
from datetime import datetime
# from config.databasehandler import DataBaseHandler
# from config.solr_query import *




def get_datetime():
    now_time = datetime.now()
    now_str = datetime.strftime(now_time, "%Y-%m-%d")
    return now_str


def upload_solr_txt(series_id, cd_solr_search, num_crawling_txt=60, is_insert=False):
    # 리뷰 insert할 테이블 지정
    # sql_insert_txt = '''
    #     insert into star_ko.star_ko_txt_data (item_key, txt_title, txt_data, txt_url, write_date, regist_date, category)
    #     values ({series_id}, "{title}", "{data}", "{url}", "{date}", "{regist_date}", "{category}")
    # '''

    sql_insert_txt = '''
        insert into star_ko.star_ko_txt_data (item_key, txt_url, txt_data, type, write_date, regist_date)
        values ({series_id}, "{url}", "{data}", "{category}", "{date}", "{regist_date}")

    '''
    try:
        results = check_solr_query(cd_solr_search, num_crawling_txt)
        txt_solrs = results[1]
        if len(txt_solrs) > 0:
            print('[INFO] series_id {}의 문서 {}개를 가져왔습니다.!!'.format(series_id, len(txt_solrs)))
        elif len(txt_solrs) == 0:
            print('[INFO] series_id {}의 문서가 없습니다.'.format(series_id))
            return ('zero', series_id)
        # ## insert text_solr into star_ko_txt_data
        for txt_solr in txt_solrs:
            # title 가져오기
            # title = txt_solr.get('title', '')
            # title = re.sub('([""''])', ' ', title)
            # content 가져오기
            content = txt_solr.get('content', '')
            content = re.sub('([""''])', ' ', content)
            content = content.replace('// flash 오류를 우회하기 위한 함수 추가function _flash_removeCallback {}', '')
            # 원본 url 가져오기
            url = txt_solr.get('url', '')
            # write date의 값을 가져오기
            date = txt_solr.get('date', '')
            date = re.sub('([0-9]{4})([0-9]{2})([0-9]{2})', '\g<1>-\g<2>-\g<3>', date)
            # regist date의 값을 가져오기
            regist_date = txt_solr.get('regist_date', '')
            regist_date = regist_date[:10]

            # txt 업로드
            if is_insert:
                dbh_star.execute_mysql(sql_insert_txt.format(
                    series_id = series_id,
                    # title = title,
                    # data = title + ' ' + content,
                    data = content,
                    url = url,
                    date = date,
                    regist_date = regist_date,
                    # category = 'news'
                    category = 'news'
                ))
        return ('done', series_id)

    except Exception as e:
        print('[ERROR] series_id {}의 문서를 가져올 수 없습니다.!!'.format(series_id))
        print(e)
        return ('error', series_id)


def artist_docs_upload(status, set_db, num_crawling_txt, is_insert_into_star_ko_txt_data, s_id=0):
    if is_insert_into_star_ko_txt_data.lower() == 'y':
        is_insert_tf = True
    if is_insert_into_star_ko_txt_data.lower() == 'n':
        is_insert_tf = False

    # ## get solr query(작업 대상이 되는 아티스트들 목록) - solr쿼리 가져오는 DB 테이블
    if status == 'new':
        if len(s_id) == 1:
            s_id_single = s_id[0]
            sql_select_solr_query = f'''
                select series_id, cd_solr_search
                from star_ko_data
                WHERE series_id = {s_id_single}
            '''
        else:
            sql_select_solr_query = f'''
                select series_id, cd_solr_search
                from star_ko_data
                WHERE series_id in {s_id}
            '''
        rows = set_db.execute_mysql(sql_select_solr_query)
    elif status == 'tot':
        sql_select_solr_query = f'''
            select series_id, cd_solr_search
            from star_ko_data 
        '''
        rows = set_db.execute_mysql(sql_select_solr_query)
    # print(rows)
    # # get solr txt
    dict_non_upload = {
        'zero': [],
        'error': []
    }
    ans = input('star_ko_txt_data에 기사내용이 대량으로 올라갑니다. 데이터, 파라미터는 모두 확인하셨나요?(y/n) ')
    if ans.lower() == 'y':
        for row in tqdm(rows):
            series_id = row['series_id']
            cd_solr_search = row['cd_solr_search']
            cd_solr_search += ' AND type: "news"'
            # ######### 아티스트별로 문서 insert #########
            result = upload_solr_txt(series_id, cd_solr_search, num_crawling_txt, is_insert=is_insert_tf)
            if result[0] == 'zero':
                dict_non_upload['zero'].append(result[1])
            if result[0] == 'error':
                dict_non_upload['error'].append(result[1])
        print('[INFO] DONE!!')
        # ## save
        print('[INFO] SAVE json file')
        save_dir = '/Users/jieun/Desktop/program/2_crawling_news/'
        with open(save_dir + 'non_upload.json', 'wt') as f:
            json.dump(dict_non_upload, f)
    else:
        # txt_data에 올라가지 않고, 멈춘다
        return


def sort_and_cut_old_docs(set_db, docu_del_criteria):
    query = f'''
        DELETE
        FROM star_ko.star_ko_txt_data
        WHERE pk IN (
            SELECT t1.pk
            FROM (SELECT t2.pk,
                         t2.item_key,
                         (CASE @vjob WHEN t2.item_key THEN @rownum := @rownum + 1 ELSE @rownum := 1 END) AS new_count,
                         concat((@vjob := t2.item_key), t2.txt_data) AS vjob
                  FROM star_ko.star_ko_txt_data AS t2, (SELECT @vjob := '', @rownum := 0 FROM DUAL) AS b
                  ORDER BY t2.item_key ASC, t2.write_date DESC) AS t1  # 아티스트키는 오름차순, 등록일은 내림차순(최신순)으로 정렬
            WHERE t1.new_count > {docu_del_criteria}  # 아티스트별로 docu_del_criteria 번째 이후 기사들은 삭제 (당분간은 60으로 고정할 생각)
        )
    '''
    set_db.execute_mysql(query)
    print('done!')

def cut_2018_docs(set_db):
    query = f'''
        DELETE
        FROM star_ko.star_ko_txt_data
        WHERE write_date < '2018-01-01'
        
    '''
    set_db.execute_mysql(query)
    print('done!')






if __name__ == '__main__':
    """
     이 작업 진행하기 전에 김종우가 1_update_data에 있는 쿼리문을 우선 실행하고, star host의 star_ko_data를 덤프 떠서 다시 dsdb에 올려야한다
    """


    dbh_kbds = DataBaseHandler(path_config='/Users/jieun/Desktop/program/config/config.ini')
    # dbh_kbds.set_mysql('star_update')
    dbh_kbds.set_mysql('star') # (2021-02-09 수정)
    dbh_star = DataBaseHandler(path_config='/Users/jieun/Desktop/program/config/config.ini')
    dbh_star.set_mysql('star')

    """1. 신규 아티스트에 대해서만 문서추가를 하는 경우 (신규 아티스트의 경우 sort_and_cut_old_docs()는 할 필요 없음"""
    date_time = get_datetime()
    new_stars = pd.read_excel(f'/Users/jieun/Desktop/program/1_update_data/result/{date_time}/star_ko.xlsx')
    new_stars = pd.read_excel(f'/Users/jieun/Desktop/program/1_update_data/result/{date_time}/star_ko.xlsx')
    new_stars_tuple = tuple(new_stars['series_id'].tolist())

    # 60은 아티스트당 사용할 문서 개수 / 'y'는 실행여부 결정(무조건 'y'로 하면 됨)
    artist_docs_upload('new', dbh_kbds, 50, 'y', new_stars_tuple)

    """2-1. 전체 아티스트에 대해서 문서추가"""
    artist_docs_upload('tot', dbh_kbds, 50, 'y')
    """2-2. dsDB에 올라간 txt_data를 최신순으로 정렬 후 50번째 이후 기사는 table에서 삭제하는 걸로"""
    sort_and_cut_old_docs(dbh_star, 50)
    # """2-3. dsDB에 올라간 txt_data 중 2018년 이 기사는 table에서 삭제하는 걸로"""
    # cut_2018_docs(dbh_star)
