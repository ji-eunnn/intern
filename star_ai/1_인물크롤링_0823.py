import pandas as pd
from bs4 import BeautifulSoup
import requests
import re
from tqdm import tqdm
from datetime import datetime
import time
import random

######################

timelabel = datetime.strftime(datetime.now(), format='%m%d_%H%M')
#timelabel

######################

excel_loc = '/Users/jieun/Desktop/업무/인물등록/인물검색.xlsx'
# excel_loc = 'C:\\Users\\kur30\\Desktop\\origin.xlsx'
excel = pd.read_excel(excel_loc)
names = excel['name']
daum_id = excel['daum']
naver_id = excel['naver']

empty_frame = pd.DataFrame(columns=('이름', '기본 분류', '성별', '검색어', '내부 검색식', '본명/영어이름', '직업', '키', '몸무게', '체질량지수', 'BMI 범위', '양력생일', '양력생일_', '음력생일', '별자리',  '띠', '나이(만)', '혈액형', '출생지', '사망일', '소속사', '소속', '소속팀', '가족', '그룹', '데뷔', '그룹/팀 여부', '멤버', '학력', '경력', '수상', '동의어', '공식사이트 [Official site]', '팬카페 [Fan Cafe]', '블로그 [Blog]', '미니홈피 [MiniHomepage]', '트위터 [Twitter]', '페이스북 [Facebook]', '인스타그램 [Instagram]', '유튜브 [Youtube]', '웨이보 [Weibo]', '텀블러 [tumblr]', '카카오스토리 [kakaostory]', '네이버캐스트 [NaverCast]', '브이라이브 [V LIVE]', '네이버토크 [NaverTalk]', '업데이트일'))
name=gender=search_keyword=true_eng_name=job=height=weight=birth=blood\
    =birth_place=dead_date=agency=belong_not_celeb=belong_sport_team=\
    family=belong_group=debut=group_flage=members=academic=career=awards=synonym=\
    official_site=fancafe=blog=cyworld=twitter=facebook=insta=youtube=\
    weibo=tumblr=kakao=naver_cast=vlive=naver_talk=''

for i in tqdm(range(len(names))):
    name = gender = search_keyword = true_eng_name = job = height = weight = birth = \
        blood = birth_place = dead_date = agency = belong_not_celeb = belong_sport_team = \
        family = belong_group = debut = group_flage = members = academic = career = awards = synonym = \
        official_site = fancafe = blog = cyworld = twitter = facebook = insta = youtube = \
        weibo = tumblr = kakao = naver_cast = vlive = naver_talk = ''

    year, month, day = '', '', ''

    ####다음####

    if not(pd.isnull(daum_id[i])):
        daum_html = requests.get(daum_id[i]).text
        soup = BeautifulSoup(daum_html, 'html.parser')

        list = soup.select('span[class=inner_summary]')
        sub_name_element = soup.select('strong[class=tit_foreign]')

        name = names[i]
        if len(sub_name_element) > 0:
            true_eng_name = sub_name_element[0].text.strip()
        for j in range(len(list)):
            title = list[j].text
            inner = list[j].parent.findNext('td').text
            print(title, inner)
            if title == '출생':
                if ',' in inner:
                    try:
                        spl = inner.split(',')[0].split()
                        birth = "{0}-{1}-{2}".format(re.sub("\D", "", spl[0]).zfill(4), re.sub("\D", "", spl[1]).zfill(2), re.sub("\D", "", spl[2]).zfill(2))
                        birth_place = inner.split(',')[1].strip()
                    except:
                        print(name, true_eng_name + "이 birth split 에서 오류 발생함.")
                        pass
                else:
                    spl = inner.split()
                    if len(spl) == 3:
                        birth = "{0}-{1}-{2}".format(re.sub("\D", "", spl[0]).zfill(4), re.sub("\D", "", spl[1]).zfill(2), re.sub("\D", "", spl[2]).zfill(2))
            elif title == "직업":
                if ',' in inner:
                    job = inner.split(',')[0].strip()
                else:
                    job = inner.strip()
            # elif title == "소속":
            #     if "선수" in job:
            #         if ',' in inner:
            #             belong_sport_team = str(', '.join([zzz.strip() for zzz in inner.split(',')]))
            #         else:
            #             belong_sport_team = inner.strip()
            #     else:
            #         if ',' in inner:
            #             try:
            #                 belong_not_celeb = inner.split(',')[0].strip()
            #             except:
            #                 belong_not_celeb = inner.strip()
            #         else:
            #             agency = inner.strip()
            elif title ==  "성별":
                if "남" in inner:
                    gender = "남자"
                elif "여" in inner:
                    gender = "여자"
                elif inner == "혼성":
                    gender = "남자,여자"
            elif title == "학력":
                academic = inner
            elif title == "소속 그룹":
                belong_group = str(', '.join([ttt.strip() for ttt in inner.strip().split(',')]))
            elif title == "데뷔":
                try:
                    debut = inner
                except:
                    pass
            elif title == "신체":
                for tmp in inner.strip().split(','):
                    if '키' in tmp:
                        height = re.sub("\D", "", tmp)
                    elif '몸무게' in tmp:
                        weight = re.sub("\D", "", tmp)
                    elif '혈액형' in tmp:
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
                if ',' in inner:
                    family = str(', '.join([zzz.strip() for zzz in inner.split(',')]))
                else:
                    family = inner.strip()
            elif "멤버" in title:
                if ',' in inner:
                    members = str(', '.join([zzz.strip() for zzz in inner.split(',')]))
                else:
                    members = inner.strip()
            elif "데뷔" in title:
                if ',' in inner:
                    debut = str(', '.join([zzz.strip() for zzz in inner.split(',')]))
                else:
                    debut = inner.strip()
            elif "학력" in title:
                if ',' in inner:
                    academic = str(', '.join([zzz.strip() for zzz in inner.split(',')]))
                else:
                    academic = inner.strip()

            etc = soup.find_all("th", {"class": "tit_profile"})
            for index in range(len(etc)):
                if "경력" in etc[index].text:
                    career = ' '.join([q.text for q in etc[index].parent.find_all("tr", {"class": re.compile('fst.*')})[0].select('td') if len(q.text) > 2])
                elif "수상" in etc[index].text:
                    awards = ' '.join([q.text for q in etc[index].parent.find_all("tr", {"class": re.compile('fst.*')})[0].select('td') if len(q.text) > 2])


    ####네이버####

    if not(pd.isnull(naver_id[i])):
        naver_html = requests.get(naver_id[i]).text
        #naver_html = requests.get(naver_url).text
        #naver_html = requests.get(naver_search_base_url + str(names[i]) + naver_search_tail_url + str(naver_id[i])).text
        naver_soup = BeautifulSoup(naver_html, 'html.parser')

        name = names[i]
        profile_dsc = naver_soup.select('div[class=profile_dsc]')
        if job ==  '':
            try:
                job = profile_dsc[0].select('dd[class=sub]')[0].text
            except:
                pass
        if true_eng_name == '':
            try :
                true_eng_name = profile_dsc[0].select('dt[class=name]')[0].select('em')[0].text
                true_eng_name = true_eng_name.replace("(", "")
                true_eng_name = true_eng_name.replace(")", "")
                true_eng_name = true_eng_name.replace(", ", "/")
            except :
                pass
        dt_in_profile_dsc_dsc = profile_dsc[0].select('dl[class=dsc]')[0].select('dt')
        dd_in_profile_dsc_dsc = profile_dsc[0].select('dl[class=dsc]')[0].select('dd')
        for row_len in range(len(dt_in_profile_dsc_dsc)):
            title = dt_in_profile_dsc_dsc[row_len].text
            inner = dd_in_profile_dsc_dsc[row_len]

            if '출생' in title:
                if birth ==  '':
                    if ',' in inner.text:
                        tmp_birth, tmp_born_place = inner.text.split(',')[0].split(), inner.text.split(',')[1]
                    else:
                        tmp_birth = inner.text.split()
                    for t in tmp_birth:
                        if '년' in t:
                            year = re.sub("\D", "", t)
                        elif '월' in t:
                            month = re.sub("\D", "", t)
                        elif '일' in t:
                            day = re.sub("\D", "", t)
                    if year != '' and month != '' and day != '':
                        birth = "{0}-{1}-{2}".format(year.zfill(4), month.zfill(2), day.zfill(2))
                    else:
                        pass
                else:
                    pass
            elif '그룹' in title:
                if belong_group == '':
                    belong_group = inner.text
            elif title == '소속사':
                if agency == '':
                    agency = inner.text.strip()
            elif title == '소속' :
                if belong_not_celeb == '':
                    belong_not_celeb = inner.text
                    belong_not_celeb = belong_not_celeb.lstrip()
                    # belong_not_celeb = belong_not_celeb.replace(" ", "")
            elif title == '소속팀' :
                if belong_sport_team == '':
                    belong_sport_team = inner.text
                    belong_sport_team = belong_sport_team.lstrip()
                    # belong_sport_team = belong_sport_team.replace(" ", "")
            elif '신체' in title:
                tmp_body = inner.text.split(',')
                for it_tmp_body in tmp_body:
                    if 'cm' in it_tmp_body:
                        if height == '':
                            height = re.sub("\D", "", it_tmp_body)
                    elif 'kg' in it_tmp_body:
                        if weight == '':
                            weight = re.sub("\D", "", it_tmp_body)
            elif '데뷔' in title:
                if debut == '':
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
                    elif "트위터" in a.text and twitter== '':
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
                if members == '': members = str(', '.join([ttt.strip() for ttt in inner.text.strip().split(',')]))

        h3_in_profile_dsc_dsc = naver_soup.select('div[class=record_wrap]')[0].select('h3[class=blind]')
        d1_in_profile_dsc_dsc = naver_soup.select('div[class=record_wrap]')[0].select('dl')

        # for v in range(len(h3_in_profile_dsc_dsc)):
        try :
            for v in range(len(d1_in_profile_dsc_dsc)):
                title = h3_in_profile_dsc_dsc[v].text
                inner = d1_in_profile_dsc_dsc[v].select('p')

                if '경력' in title:
                    career = inner[0].text

                elif '학력' in title:
                    academic = inner[0].text

                elif '수상' in title:
                    awards = inner[0].text
        except :
            pass


    # time.sleep(random.uniform(0.5, 3))

    if birth != '' and '00' in birth[5:]:
        birth = ''

    if members != '':
        group_flage = '1'
    else:
        group_flage = '0'
        print("그룹/팀 여부 에러")

    true_eng_name = true_eng_name.replace("\t", "")
    true_eng_name = true_eng_name.replace("\n", "")
    true_eng_name = true_eng_name.replace(",", "/")

    job = job.replace(" ", "")
    job = job.replace("탤런트", "배우")
    job = job.replace("음악인", "가수")
    job = job.replace("코미디언", "개그맨")
    search_keyword = f"{job}\"{name}\""
          
    final_list = [name, job, gender, search_keyword, '', true_eng_name, job, height, weight, '', '',
                          birth, '', '', '', '', '', blood, birth_place, dead_date, agency, belong_not_celeb, belong_sport_team,
                          family, belong_group, debut, group_flage, members, academic, career, awards, synonym, official_site, fancafe,
                          blog, cyworld, twitter, facebook, insta, youtube, weibo, tumblr, kakao, naver_cast, vlive, naver_talk, '']

    for final in final_list:
        if '\t' in final:
            final = final.replace('\t', '')

    empty_frame.loc[i] = final_list
    print(empty_frame)

writer = pd.ExcelWriter('/Users/jieun/Desktop/업무/인물등록/아카이브/인물크롤링/인물_크롤링_'+timelabel+'.xlsx', engine='openpyxl')
empty_frame.to_excel(writer, sheet_name='Sheet1')
writer.save()
writer.close()