import pandas as pd
from bs4 import BeautifulSoup
import requests
import datetime
import urllib.request as urllib
import glob
from PIL import Image  # pip install pillow
import os


def mkdir(imgpath) :
    try :
        os.mkdir(imgpath)
    except FileExistsError :
        pass

def imgDownload(excel) :
    naverBlank = 'https://ssl.pstatic.net/sstatic/search/images11/blank.gif'
    null_data = []

    for i, row in excel.iterrows():
        name = row['name']
        daum_id = row['daum']
        naver_id = row['naver']
        cd_idx = row['cd_idx']

        ## 네이버 이미지 추출
        if pd.isnull(naver_id):
            profileImg = ''
            null_data.append(cd_idx)
            ## 다음 이미지 추출
        #         try :
        #             daum_html = requests.get(daum_id).text
        #             daum_soup = BeautifulSoup(daum_html, 'html.parser')
        #             daum_imgSource = daum_soup.find('a', {'class':'link_figure'})
        #             if img_source is None :
        #                 profileImg = ''
        #             else :
        #                 profileImg = daum_imgSource.find('img')['src']
        #         except :
        #             profileImg = ''
        else:
            try:
                naver_html = requests.get(naver_id).text
                naver_soup = BeautifulSoup(naver_html, 'html.parser')
                naver_imgSource = naver_soup.find('div', {'class': 'profile_wrap'}).find('img')['src']

                if naver_imgSource != naverBlank:
                    profileImg = naver_imgSource
                else:
                    profileImg = ''
                    null_data.append(cd_idx)

            except:
                profileImg = ''
                null_data.append(cd_idx)

        if profileImg == '':
            print("{0}-{1} 이미지 작업 필요".format(cd_idx, name))
        else:
            urllib.urlretrieve(profileImg, f"{imgpath}/{cd_idx}_sq.jpg")

    print(f"[INFO] 총 {len(null_data)} 개 이미지 작업 필요")
    print("[INFO] image Download DONE !")


def imgCrop() :
    for root, dirs, files in os.walk(imgpath):
        for file in files:
            try:
                img = Image.open(root + "/" + file)

                # 네이버 솔로 이미지
                if img.size == (120, 150):
                    image_crop = img.crop((0, 20, 120, 140))
                # 네이버 그룹 이미지
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
    if ans.lower() == 'y' :
        imgDownload(excel)
        imgCrop()
        imgResize()

    else :
        return



if __name__ == '__main__':
    now = datetime.datetime.now()
    date_ = now.strftime('%y%m%d')

    imgpath = f'/Users/jieun/Desktop/업무/인물등록/아카이브/이미지등록전/{date_}'
    mkdir(imgpath)

    excel_loc = '/Users/jieun/Desktop/업무/인물등록/인물검색.xlsx'
    excel = pd.read_excel(excel_loc)

    img()