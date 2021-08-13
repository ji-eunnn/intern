from PIL import Image
import os
import pandas as pd
from datetime import datetime


now = datetime.now()
now_year = str(now.year)[2:4]
now_month = str(now.month).zfill(2)
now_day = str(now.day).zfill(2)

#이미지 담당자 분이 편집해준 파일 불러온 후 사이즈 검수 (경로 변경 필요)

file_list2 = os.listdir(f'/Users/jieun/Desktop/스타이미지_편집본_{now_year}{now_month}{now_day}')
# file_list2 = os.listdir(f'/Users/mycelebs/Desktop/스타이미지_편집본_210503')
for j in file_list2:
    try:
        filedir = f'/Users/jieun/Desktop/스타이미지_편집본_{now_year}{now_month}{now_day}/{j}/'
        # filedir = f'/Users/mycelebs/Desktop/스타이미지_편집본_210503/{j}/'
        file_list = os.listdir(f'/Users/jieun/Desktop/스타이미지_편집본_{now_year}{now_month}{now_day}/{j}')
        # file_list = os.listdir(f'/Users/mycelebs/Desktop/스타이미지_편집본_210503/{j}')
        for i in file_list:
            try:
                im = Image.open(filedir+i)
                img2 = im.resize((int(float(i.split('@')[0].split('_')[1])), int(float(i.split('@')[1].split('.')[0]))))
                img2.save(f'/Users/jieun/Desktop/스타이미지_편집본_{now_year}{now_month}{now_day}/{j}/{i[:-4]}.jpg')
                # img2.save(f'/Users/mycelebs/Desktop/스타이미지_편집본_210503/{j}/{i[:-4]}.jpg')
            except:
                print(f'{i}')
    except:
        pass

