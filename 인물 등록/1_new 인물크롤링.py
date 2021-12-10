'''
(21/09/01)
1. 크롤링시 네이버 메인으로 이동하는 이슈 발생
 맥시멈 20번동안 재진입 시도, 대게 2~3번 시도하면 들어가진다

(21/09/07)
1. 네이버 우선으로 수정 -> 네이버 크롤링 먼저 하고, 값이 비어있는 경우에만 다음에서 가져온다.
2. 키 & 몸무게 소수점도 가져오도록 수정

(21/09/14)
1. 네이버 인물사전 프로필 페이지가 사라짐...
  네이버 검색창에 인물 검색했을 때 뜨는 첫 페이지에서 크롤링 해오는걸로...
  바뀐 url 양식 -> https://search.naver.com/search.naver?where=nexearch&sm=tab_etc&mra=bjky&pkid=1&os={고유숫자}&qvt=0&query={이름}

(21/09/24)
1. 사망 여부도 크롤링

(21/10/18)
1. 출생지도 크롤링

(21/10/21)
1. star_ko_data에 naver_people_id(네이버 프로필 고유값), daum_people_id(다음 프로필 고유값) 컬럼 추가됨
  -> 크롤링 과정에서 각 프로필 url의 고유값들까지 엑셀에 출력하도록 수정

(21/10/26)
1. 매칭 단어 로직 추가

(21/12/03)
1. 간헐적으로 네이버 url 진입이 실패하는 경우 발생
    맥시멈 10번동안 재진입 시도
'''

import pandas as pd
from bs4 import BeautifulSoup
import requests
import re
from tqdm import tqdm
from datetime import datetime
import time
import random

timelabel = datetime.strftime(datetime.now(), format='%m%d_%H%M')
reg = re.compile(r'[a-zA-Z]')

excel_loc = '/Users/jieun/Desktop/업무/인물등록/인물검색.xlsx'
excel = pd.read_excel(excel_loc)
names = excel['name']
daum_id = excel['daum']
naver_id_ = excel['naver']
naver_id = naver_id_ + '프로필'

empty_frame = pd.DataFrame(columns=('이름', '기본 분류', '성별', '검색어', '매칭 단어', '내부 검색식', '본명/영어이름', '직업', '키', '몸무게', '체질량지수', 'BMI 범위', '양력생일', '양력생일_', '음력생일', '별자리',  '띠', '나이(만)', '혈액형', '출생지', '사망일', '소속사', '소속', '소속팀', '가족', '그룹', '데뷔', '그룹/팀 여부', '멤버', '학력', '경력', '수상', '동의어', '사망 여부', '네이버 인물 id', '다음 인물 id', '네이버 영화 인물 id', '광고정보센터 인물 id', '플레이디비 인물 id', '바이브 인물 id', '공식사이트 [Official site]', '팬카페 [Fan Cafe]', '블로그 [Blog]', '미니홈피 [MiniHomepage]', '트위터 [Twitter]', '페이스북 [Facebook]', '인스타그램 [Instagram]', '유튜브 [Youtube]', '웨이보 [Weibo]', '텀블러 [tumblr]', '카카오스토리 [kakaostory]', '네이버캐스트 [NaverCast]', '브이라이브 [V LIVE]', '네이버토크 [NaverTalk]', '업데이트일'))
name=gender=search_keyword=match=true_eng_name=job=height=weight=birth=blood\
    =birth_place=dead_date=agency=belong_not_celeb=belong_sport_team=\
    family=belong_group=debut=group_flage=members=academic=career=awards=synonym=death=\
    official_site=fancafe=blog=cyworld=twitter=facebook=insta=youtube=\
    weibo=tumblr=kakao=naver_cast=vlive=naver_talk=''

for i in tqdm(range(len(names))):
    name = gender = search_keyword = match = true_eng_name = job = height = weight = birth = \
        blood = birth_place = dead_date = agency = belong_not_celeb = belong_sport_team = \
        family = belong_group = debut = group_flage = members = academic = career = awards = synonym = death = \
        official_site = fancafe = blog = cyworld = twitter = facebook = insta = youtube = \
        weibo = tumblr = kakao = naver_cast = vlive = naver_talk = ''

    year, month, day = '', '', ''
    name = str(names[i])

    try:
        daum_people_id = re.findall('view/(\S+)', daum_id[i])[0]
    except:
        daum_people_id = ''
    try:
        naver_people_id = re.findall('&os=(\d+)', naver_id[i])[0]
    except:
        naver_people_id = ''

    ####네이버####

    if not (pd.isnull(naver_id[i])):

        cnt = 0
        while cnt < 10 :
            cnt += 1
            try :
                naver_html = requests.get(naver_id[i]).text
                naver_soup = BeautifulSoup(naver_html, 'html.parser')

                # 본명 & 직업
                # 자꾸 sub_title 에서 IndexError 남
                try :
                    sub_title = naver_soup.select('div[class="sub_title first_elss"]')[0].select('span[class=txt]')
                    if len(sub_title) == 2:
                        true_eng_name = sub_title[0].text
                        job = sub_title[1].text

                    elif len(sub_title) == 1:
                        # true_eng_name = name
                        job = sub_title[0].text

                    dt_in_info_txt = naver_soup.select('div[class="detail_info"]')[0].select('dt')
                    dd_in_info_txt = naver_soup.select('div[class="detail_info"]')[0].select('dd')

                    for row_len in range(len(dt_in_info_txt)):
                        title = dt_in_info_txt[row_len].text
                        inner = dd_in_info_txt[row_len]

                        # 생일 & 출생지
                        if '출생' in title:
                            if ',' in inner.text:
                                tmp_birth = inner.text.split(',')[0].split()
                            else:
                                tmp_birth = inner.text.split()

                            if len(tmp_birth) == 1 :
                                year = re.sub("\D", "", tmp_birth[0])
                                month, day = '00'
                            elif len(tmp_birth) >= 4 :
                                year = re.sub("\D", "", tmp_birth[0])
                                month = re.sub("\D", "", tmp_birth[1])
                                day = re.sub("\D", "", tmp_birth[2])
                                birth_place = tmp_birth[3]
                            else :
                                year = re.sub("\D", "", tmp_birth[0])
                                month = re.sub("\D", "", tmp_birth[1])
                                day = re.sub("\D", "", tmp_birth[2])

                            if year != '' and month != '' and day != '':
                                birth = "{0}-{1}-{2}".format(year.zfill(4), month.zfill(2), day.zfill(2))

                        elif '사망' in title :
                            death = '1'

                        elif '그룹' in title:
                            belong_group = inner.text
                            belong_group = belong_group.strip()

                        elif title == '소속사':
                            agency = inner.text.strip()

                        elif title == '소속':
                            belong_not_celeb = inner.text
                            belong_not_celeb = belong_not_celeb.lstrip()

                        elif title == '소속팀':
                            belong_sport_team = inner.text
                            belong_sport_team = belong_sport_team.lstrip()

                        elif '신체' in title:
                            tmp_body = inner.text.split(',')
                            for it_tmp_body in tmp_body:
                                if 'cm' in it_tmp_body:
                                    if height == '':
                                        height = re.findall('\d+\.?\d+?', it_tmp_body)[0]
                                elif 'kg' in it_tmp_body:
                                    if weight == '':
                                        weight = re.findall('\d+\.?\d+?', it_tmp_body)[0]

                        elif '데뷔' in title:
                            debut = inner.text.strip()

                        elif '사이트' in title:
                            a_lists = inner.select('a')
                            for a in a_lists:
                                if "팬" in a.text and fancafe == '':
                                    fancafe = a['href']
                                elif "공식" in a.text and official_site == '':
                                    official_site = a['href']
                                elif "블로그" in a.text and blog == '':
                                    blog = a['href']
                                elif "홈피" in a.text and cyworld == '':
                                    cyworld = a['href']
                                elif "트위터" in a.text and twitter == '':
                                    twitter = a['href']
                                elif "페이스" in a.text and facebook == '':
                                    facebook = a['href']
                                elif "인스타" in a.text and insta == '':
                                    insta = a['href']
                                elif "유튜" in a.text and youtube == '':
                                    youtube = a['href']
                                elif "웨이" in a.text and weibo == '':
                                    weibo = a['href']
                                elif "텀블" in a.text and tumblr == '':
                                    tumblr = a['href']
                                elif "카카오" in a.text and kakao == '':
                                    kakao = a['href']
                                elif ("브이" in a.text or 'live' in a.text or 'LIVE' in a.text) and vlive == '':
                                    vlive = a['href']
                                elif ("토크" in a.text or 'talk' in a.text) and naver_talk == '':
                                    naver_talk = a['href']
                                elif "캐스트" in a.text and naver_cast == '':
                                    naver_cast = a['href']

                        elif "멤버" in title:
                            members = str(', '.join([ttt.strip() for ttt in inner.text.strip().split(',')]))
                            members = re.sub(r'\([^)]*\)', '', members)  # 괄호, 괄호 안 문자 삭제 ex. 온유(리더), 종현(메인보컬), ...

                    # 학력
                    try:
                        content_area_school = naver_soup.select('div[class="cm_content_area _cm_content_area_school"]')
                        academic = content_area_school[0].select('dd')[0].text
                        academic = academic.strip()
                    except:
                        academic = ''

                    # 수상
                    try:
                        content_area_prize = naver_soup.select('div[class="cm_content_area _cm_content_area_prize"]')
                        awards = content_area_prize[0].select('dd')[0].text
                        awards = awards.strip()
                    except:
                        awards = ''

                    # 경력
                    try:
                        content_area_career = naver_soup.select('div[class="cm_content_area _cm_content_area_career"]')
                        career = content_area_career[0].select('dd')[0].text
                        career = career.strip()
                    except:
                        career = ''


                except:

                    naver_html_ = requests.get(naver_id_[i]).text
                    naver_soup_ = BeautifulSoup(naver_html_, 'html.parser')

                    menu = naver_soup_.select('span[class="menu"]')

                    menu_nm = []

                    for m in range(len(menu)):
                        menu_nm_ = menu[m].text
                        menu_nm.append(menu_nm_)

                    if '프로필' in menu_nm:
                        print(name, "크롤링 에러")

                    sub_title = naver_soup_.select('div[class="sub_title first_elss"]')[0].select('span[class=txt]')

                    if len(sub_title) == 2:
                        true_eng_name = sub_title[0].text
                        job = sub_title[1].text

                    elif len(sub_title) == 1:
                        true_eng_name = name
                        job = sub_title[0].text

            except :
                print("! url 재진입 시도 중 ~")
                continue
            break


    ####다음####

    if not (pd.isnull(daum_id[i])):
        daum_html = requests.get(daum_id[i]).text
        soup = BeautifulSoup(daum_html, 'html.parser')

        list = soup.select('span[class=inner_summary]')
        sub_name_element = soup.select('strong[class=tit_foreign]')

        name = str(names[i])

        if true_eng_name == '':
            if len(sub_name_element) > 0:
                true_eng_name = sub_name_element[0].text.strip()

        for j in range(len(list)):
            title = list[j].text
            inner = list[j].parent.findNext('td').text
            print(title, inner)
            if title == '출생':
                if birth == '':
                    if ',' in inner:
                        try:
                            spl = inner.split(',')[0].split()
                            birth = "{0}-{1}-{2}".format(re.sub("\D", "", spl[0]).zfill(4),
                                                         re.sub("\D", "", spl[1]).zfill(2),
                                                         re.sub("\D", "", spl[2]).zfill(2))
                            birth_place = inner.split(',')[1].strip()
                        except:
                            print(name, true_eng_name + "이 birth split 에서 오류 발생함.")
                            pass
                    else:
                        spl = inner.split()
                        if len(spl) == 3:
                            birth = "{0}-{1}-{2}".format(re.sub("\D", "", spl[0]).zfill(4),
                                                         re.sub("\D", "", spl[1]).zfill(2),
                                                         re.sub("\D", "", spl[2]).zfill(2))

            elif '사망' in title :
                if death == '' :
                    death == '1'

            elif title == "직업":
                if job == '':
                    if ',' in inner:
                        job = inner.split(',')[0].strip()
                    else:
                        job = inner.strip()
            elif title == "소속":
                if "선수" in job:
                    if belong_sport_team == '':
                        if ',' in inner:
                            belong_sport_team = str(', '.join([zzz.strip() for zzz in inner.split(',')]))
                        else:
                            belong_sport_team = inner.strip()
                else:
                    if belong_not_celeb == '':
                        if ',' in inner:
                            try:
                                belong_not_celeb = inner.split(',')[0].strip()
                            except:
                                belong_not_celeb = inner.strip()
                        else:
                            pass
                            # if agency == '':
                            #     agency = inner.strip()
            elif title == "성별":
                if "남" in inner:
                    gender = "남자"
                elif "여" in inner:
                    gender = "여자"
                elif inner == "혼성":
                    gender = "남자,여자"
            elif title == "학력":
                if academic == '':
                    academic = inner
            elif title == "소속 그룹":
                if belong_group == '':
                    belong_group = str(', '.join([ttt.strip() for ttt in inner.strip().split(',')]))
                    belong_group = belong_group.strip()
            elif title == "데뷔":
                if debut == '':
                    try:
                        debut = inner
                    except:
                        pass
            elif title == "신체":
                for tmp in inner.strip().split(','):
                    if '키' in tmp:
                        if height == '':
                            # height = re.sub("\D", "", tmp)
                            height = re.findall('\d+\.?\d+?', tmp)[0]
                    elif '몸무게' in tmp:
                        if weight == '':
                            # weight = re.sub("\D", "", tmp)
                            weight = re.findall('\d+\.?\d+?', tmp)[0]
                    elif '혈액형' in tmp:
                        if blood == '':
                            blood = (re.compile('[^A-Z]+')).sub("", tmp) + "형"
            elif title == "사이트":
                a_sites = [ttmp for ttmp in soup.select('a[class=link_summary]') if ttmp.has_attr('title')]
                for it_asites in a_sites:
                    if "공식" in it_asites.text:
                        official_site = it_asites['href']
                    elif "팬" in it_asites.text:
                        fancafe = it_asites['href']
                    elif "블로그" in it_asites.text:
                        blog = it_asites['href']
                    elif "홈피" in it_asites.text:
                        cyworld = it_asites['href']
                    elif "트위터" in it_asites.text:
                        twitter = it_asites['href']
                    elif "페이스" in it_asites.text:
                        facebook = it_asites['href']
                    elif "인스타" in it_asites.text:
                        insta = it_asites['href']
                    elif "유튜" in it_asites.text:
                        youtube = it_asites['href']
                    elif "웨이" in it_asites.text:
                        weibo = it_asites['href']
                    elif "텀블" in it_asites.text:
                        tumblr = it_asites['href']
                    elif "카카오" in it_asites.text:
                        kakao = it_asites['href']
            elif "인물" in title:
                if family == '':
                    if ',' in inner:
                        family = str(', '.join([zzz.strip() for zzz in inner.split(',')]))
                    else:
                        family = inner.strip()
            elif "멤버" in title:
                if members == '':
                    if ',' in inner:
                        members = str(', '.join([zzz.strip() for zzz in inner.split(',')]))
                    else:
                        members = inner.strip()
            elif "데뷔" in title:
                if debut == '':
                    if ',' in inner:
                        debut = str(', '.join([zzz.strip() for zzz in inner.split(',')]))
                    else:
                        debut = inner.strip()
            elif "학력" in title:
                if academic == '':
                    if ',' in inner:
                        academic = str(', '.join([zzz.strip() for zzz in inner.split(',')]))
                    else:
                        academic = inner.strip()

            etc = soup.find_all("th", {"class": "tit_profile"})
            for index in range(len(etc)):
                if "경력" in etc[index].text:
                    if career == '':
                        career = ' '.join([q.text for q in
                                           etc[index].parent.find_all("tr", {"class": re.compile('fst.*')})[
                                               0].select('td') if len(q.text) > 2])
                elif "수상" in etc[index].text:
                    if awards == '':
                        awards = ' '.join([q.text for q in
                                           etc[index].parent.find_all("tr", {"class": re.compile('fst.*')})[
                                               0].select('td') if len(q.text) > 2])

    # time.sleep(random.uniform(0.5, 3))

    if birth != '' and '00' in birth[5:]:
        birth = ''

    if members != '':
        group_flage = '1'
    else:
        group_flage = '0'
        print("그룹/팀 여부 에러")

    if death != '1':
        death = '0'

    job = job.replace(" ", "")
    job = job.replace("탤런트", "배우")
    job = job.replace("음악인", "가수")
    job = job.replace("코미디언", "개그맨")
    job = job.replace("온라인콘텐츠창작자", "크리에이터")
    job = job.replace("방송연예인", "방송인")

    if (job == "배우,영화배우") | (job == "영화배우,배우") :
        job = "배우"
    elif (job == "배우,연극배우") | (job == "연극배우,배우") :
        job = "연극배우"
    elif (job == "배우,뮤지컬배우") | (job == "뮤지컬배우,배우") :
        job = "뮤지컬배우"

    search_keyword = f"{job}\"{name}\""

    true_eng_name = true_eng_name.replace("\t", "")
    true_eng_name = true_eng_name.replace("\n", "")
    true_eng_name = true_eng_name.replace(", ", "/")

    if true_eng_name == name :
        true_eng_name = ''


    #### 매칭 단어

    job_ = job
    job_ = job_.replace("코미디언", "개그맨,코미디언")
    job_ = job_.replace("크리에이터", "유튜버")
    job_ = job_.replace("연극배우", "배우")
    job_ = job_.replace("뮤지컬배우", "배우")
    job_ = job_.replace("영화배우", "배우")
    job_ = job_.replace("영화감독", "감독")
    job_ = job_.replace("시나리오작가", "작가")
    job_ = job_.replace("드라마작가", "작가")
    job_ = job_.replace("전PD", "PD")
    job_ = job_.replace("대학교수", "교수")

    # '배우'가 중복되면 (ex. 영화배우, 연극배우) '배우'만 남도록
    cnt = 0
    for jobb in job_.split(',') :
        if '배우' in jobb :
            cnt += 1
    if cnt == 2 :
        job_ = '배우'

    cnt = 0
    for jobb in job_.split(',') :
        if '작가' in jobb :
            cnt += 1
    if cnt == 2 :
        job_ = '작가'

    # 영어 이름
    if true_eng_name != '' :
        if reg.match(true_eng_name):  ## 본명/영어이름이 영어로만 이루어져 있으면
            true_eng_name_ = true_eng_name
            if '/' in true_eng_name :
                true_eng_name_ = true_eng_name.replace('/', ', ')
            elif ',' in true_eng_name :
                true_eng_name_ = true_eng_name.replace(',', ', ')
            match = name + ', ' + true_eng_name_

    # 그룹명
    elif belong_group != '':
        if len(belong_group.split(',')) > 1:
            match_1 = belong_group.split(',')[0] + ' ' + name
            match_2 = belong_group.split(',')[1] + ' ' + name
            match = match_1 + ', ' + match_2
            try :
                match_3 = belong_group.split(',')[2] + ' ' + name
                match = match + ', ' + match_3
            except:
                pass
        else:
            match = belong_group + ' ' + name

    # 소속
    elif belong_not_celeb != '' :
        # match = match + ', ' + belong_not_celeb + ' ' + name
        # match = belong_not_celeb + ' ' + name
        match = belong_not_celeb + name

    else :
        if len(job_.split(',')) > 1:
            match_1 = job_.split(',')[0] + ' ' + name
            match_2 = job_.split(',')[1] + ' ' + name
            match = match_1 + ', ' + match_2

            for jjob in job_.split(','):
                if '감독' in jjob or 'PD' in jjob or '작가' in jjob or '아나운서' in jjob or '교수' in jjob or '디자이너' in jjob:
                    match_ = name + ' ' + jjob
                    match = match + ', ' + match_

                elif '선수' in jjob or '골퍼' in jjob :
                    match_ = name + ' 선수'
                    match = match + ', ' + match_

                elif '의원' in jjob :
                    match_ = name + ' 의원'
                    match = match + ', ' + match_

                elif '프로듀서' in jjob :
                    match_ = name + ' ' + jjob
                    match = match + ', ' + match_ + ', PD ' + name + ', ' + name + ' PD'

        else:
            if '감독' in job_ or 'PD' in job_ or '작가' in job_ or '아나운서' in job_ or '교수' in job_ or '디자이너' in job_:
                match = job_ + ' ' + name + ', ' + name + ' ' + job_

            elif '선수' in job_ or '골퍼' in job_ :
                match = job_ + ' ' + name + ', ' + name + ' 선수'

            elif '의원' in job_ :
                match = job_ + ' ' + name + ', ' + name + ' 의원'

            elif '프로듀서' in job_ :
                match = job_ + ' ' + name + ', PD ' + name + ', ' + name + ' PD'

            else :
                match = job_ + ' ' + name


    final_list = [name, job, gender, search_keyword, match, '', true_eng_name, job, height, weight, '', '',
                  birth, '', '', '', '', '', blood, birth_place, dead_date, agency, belong_not_celeb,
                  belong_sport_team,
                  family, belong_group, debut, group_flage, members, academic, career, awards, synonym, death,
                  naver_people_id, daum_people_id, '', '', '', '',
                  official_site, fancafe,
                  blog, cyworld, twitter, facebook, insta, youtube, weibo, tumblr, kakao, naver_cast, vlive,
                  naver_talk, '']

    # for final in final_list:
    #     if '\t' in final:
    #         final = final.replace('\t', '')

    empty_frame.loc[i] = final_list
    print(empty_frame)

writer = pd.ExcelWriter(
    '/Users/jieun/Desktop/업무/인물등록/아카이브/인물크롤링/인물_크롤링_' + timelabel + '.xlsx',
    engine='openpyxl')
empty_frame.to_excel(writer, sheet_name='Sheet1')
writer.save()
writer.close()










