
'''
목표 : 신규 등록 인물 리스트 생성 & 프로필 사진 등록

프로세스 :
-> 2_celeb등록에서 인물등록이 완료되면, 등록한 신규 인물 리스트가 필요함
-> 도넛 > 셀럽 관리에서 일일히 복붙해도 되지만, 편의를 위해 셀레니움으로 오늘 날짜에 등록된 스타들의 series_id와 이름을 크롤링 해온다
-> 최대 20페이지까지 크롤링을 해오는데, 등록한 개체가 적거나 많은 경우 유동적으로 수정하면 된다 - make_image_list()
-> 리스트가 생성되면, 오름차순으로 정렬 후 1차로 복사하여 슬랙에 보고하고, 2차로 인물등록 시트와 인물검색.xlsx 에 series_id를 붙여넣는다
-> 인물검색.xlsx 으로부터 프로필 사진을 다운로드 및 크롭, 리사이징을 하고,
-> 4_이미지 등록.py으로 넘어가서 작업한 이미지 파일들을 해당 개체에 등록시킨다


'''

import urllib.request as urllib
import glob
from PIL import Image  # pip install pillow
import os
from selenium import webdriver
import requests
import time
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime


def make_image_list() :
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--headless')
    driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)

    pid =
    ppw =
    driver.get(f'http://{pid}:{ppw}@dev.mycelebs.com/donut/')
    alert = driver.switch_to.alert
    alert.accept()
    time.sleep(2.0)

    driver.find_element_by_css_selector('#adminId').send_keys('')
    driver.find_element_by_css_selector('#adminPw').send_keys('')
    btn = driver.find_element_by_css_selector('#loginForm > button')
    btn.click()

    people = []

    # 페이지 수 - 필요에 따라 유동적으로 조절
    for i in range(1, 20):
        url = f'https://dev.mycelebs.com/donut/Celeb/ShowList?opt_vertical=celeb_data&opt=cd_name&keyword=&page={i}'
        driver.get(url)

        source = driver.page_source
        soup = BeautifulSoup(source, "html.parser")

        for j in range(1, 21):   # 고정
            date = \
            soup.select(f'body > div.container > div > div > table > tbody > tr:nth-child({j}) > td:nth-child(5)')[
                0].text
            date = date[0:10]

            if date == date1:
                cd_idx = \
                soup.select(f'body > div.container > div > div > table > tbody > tr:nth-child({j}) > td:nth-child(1)')[
                    0].text
                name = soup.select(
                    f'body > div.container > div > div > table > tbody > tr:nth-child({j}) > td:nth-child(2) > a')[
                    0].text
                people_ = {'cd_idx': cd_idx, 'name': name}
                people.append(people_)

        print(i, "페이지 크롤링 완료")


    df = pd.DataFrame(people)
    print(df)
    df.to_excel('/Users/jieun/Desktop/업무/인물등록/image_list.xlsx', index=False)

    print("[INFO] image_list.xlsx 생성 완료 !")

    driver.close()


def mkdir(imgpath) :
    try :
        os.mkdir(imgpath)
    except FileExistsError :
        pass


def imgDownload(excel) :
    null_data = []

    for i, row in excel.iterrows():
        name = row['name']
        naver_id = row['naver']
        cd_idx = row['cd_idx']

        # 네이버 이미지 추출
        if pd.isnull(naver_id):
            profileImg = ''
            null_data.append(cd_idx)

        else :
            naver_html = requests.get(naver_id).text
            naver_soup = BeautifulSoup(naver_html, 'html.parser')

            try:
                profileImg = naver_soup.select('div[class="detail_info"]')[0].find('img')['src']

            except :
                profileImg = ''
                null_data.append(cd_idx)

            if profileImg == '' :
                print("{0}-{1} 이미지 작업 필요".format(cd_idx, name))

            else :
                try :
                    urllib.urlretrieve(profileImg, f"{imgpath}/{cd_idx}_sq.jpg")
                except :   # HTTP ERROR 404
                    pass

    print(f"[INFO] 총 {len(null_data)} 개 이미지 작업 필요")
    print("[INFO] image Download DONE !")


def imgCrop() :
    for root, dirs, files in os.walk(imgpath):
        for file in files:
            try:
                img = Image.open(root + "/" + file)

                # 네이버 솔로 이미지
                if img.size == (173, 216):
                    image_crop = img.crop((0, 37, 173, 210))
                # 네이버 그룹 이미지
                elif img.size == (450, 300):
                    image_crop = img.crop((75, 0, 375, 300))
                # 구네이버 솔로 이미지
                elif img.size == (120, 150):
                    image_crop = img.crop((0, 20, 120, 140))
                elif img.size == (158, 216):
                    image_crop = img.crop((0, 40, 158, 198))
                # 구네이버 그룹 이미지
                elif img.size == (225, 150):
                    image_crop = img.crop((50, 0, 200, 150))
                else:
                    # print(file, "이미지 크롭 작업 필요")
                    image_crop = img

                image_crop.save(root + "/" + file)

            except:
                pass

    print("[INFO] image Crop DONE !")


# 이미지 리사이즈
def imgResize() :
    files = glob.glob(f'{imgpath}/*.jpg')
    for f in files:
        img = Image.open(f)
        img_resize = img.resize((320, 320))
        img_resize.save(f)

    print("[INFO] image Resize DONE !")


def img() :
    ans = input('인물검색.xlsx 에 series_id 값을 넣었나요?(y/n)')
    if ans.lower() == 'y' or ans.lower() == 'ㅛ':
        imgDownload(excel)
        imgCrop()
        imgResize()

    else :
        return



if __name__ == '__main__':
    now = datetime.now()
    date1 = now.strftime('%Y-%m-%d')
    date2 = now.strftime('%y%m%d')

    """ 1. image_list.xlsx 생성 """
    make_image_list()


    """ 2. 인물검색.xlsx 으로부터 프로필 사진 다운로드 및 크롭, 리사이징 """

    imgpath = f'/Users/jieun/Desktop/업무/인물등록/아카이브/이미지등록전/{date2}'
    mkdir(imgpath)

    excel_loc = '/Users/jieun/Desktop/업무/인물등록/인물검색.xlsx'
    excel = pd.read_excel(excel_loc)

    img()

