
from selenium import webdriver  # pip install selenium
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import time
from datetime import datetime

# day = input('날짜 입력 :')
file = 'image_list.xlsx'
now = time.localtime()
day = str(now.tm_year)[2:] + str(now.tm_mon)[:2].zfill(2) + str(now.tm_mday)[:2].zfill(2)

date1 = datetime.now().strftime('%Y-%m-%d')

pro_excel = pd.read_excel('/Users/jieun/Desktop/업무/인물등록/'+ file)
# pro_excel = pd.read_excel('/Users/jieun/Desktop/업무/인물등록/null_data/null_image_list_2021-09-02.xlsx')
# cd_idx = pro_excel['cd_idx']

chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--incognito')
chrome_options.add_argument('headless')
## 요기
driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)
pid = 'mycelebsTempUser'
ppw = 'mycelebs!@rookie'
driver.get(f'http://{pid}:{ppw}@dev.mycelebs.com/donut/')

# alert = driver.switch_to_alert()
alert = driver.switch_to.alert
alert.accept()

# adminId
driver.find_element_by_css_selector('#adminId').send_keys('rookie')
driver.find_element_by_css_selector('#adminPw').send_keys('1234')
btn = driver.find_element_by_css_selector('#loginForm > button')
btn.click()

null_data = []

# for i in tqdm(cd_idx):
for j, row in pro_excel.iterrows() :
    i = row['cd_idx']
    name = row['name']
    imageUrl = 'http://dev.mycelebs.com/donut/CelebImage/ShowManage/' + str(i)
    driver.get(imageUrl)
    print(i)

    try :
        driver.execute_script("document.getElementById('f_mode').type='block'")  # 작업 모드 Input Tag 노출
        driver.find_element_by_css_selector('#f_mode').send_keys('insert')  # insert = 삽입, delete = 삭제
        driver.execute_script("document.getElementById('celeb_image_3').style='display:block'")  # Image Input Tag 노출
        driver.execute_script(
            "document.querySelector('#update > input[type=\"hidden\"]:nth-child(1)').type='block'")  # cd_idx 노출
        driver.execute_script(
            "document.querySelector('#update > input[type=\"hidden\"]:nth-child(4)').type='block'")  # ciu_idx 노출
        # 320 x 320 -> sq
        time.sleep(1)
        try:
            driver.find_element_by_css_selector('#celeb_image_3').clear()
            driver.find_element_by_css_selector('#celeb_image_3').send_keys(
                '/Users/jieun/Desktop/업무/인물등록/아카이브/이미지등록전/' + str(day) + '/' + str(i) + '_sq.jpg')  # 320x320 I/O 작업 실행
        except:
            try:
                driver.find_element_by_css_selector('#celeb_image_3').send_keys(
                    '/Users/jieun/Desktop/업무/인물등록/아카이브/이미지등록전/' + str(day) + '/' + str(i) + '_sq.png')  # 320x320 I/O 작업 실행
            except:
                driver.find_element_by_css_selector('#celeb_image_3').send_keys(
                    '/Users/jieun/Desktop/업무/인물등록/아카이브/이미지등록전/' + str(day) + '/' + str(i) + '_sq.jpeg')


        # 640*414 - name[i] + '414.jpg'
        driver.execute_script("document.getElementById('f_mode').type='block'")  # 작업 모드 Input Tag 노출
        driver.find_element_by_css_selector('#f_mode').send_keys('insert')  # insert = 삽입, delete = 삭제
        driver.execute_script("document.getElementById('celeb_image_1').style='display:block'")  # Image Input Tag 노출
        driver.execute_script(
            "document.querySelector('#update > input[type=\"hidden\"]:nth-child(1)').type='block'")  # cd_idx 노출
        driver.execute_script(
            "document.querySelector('#update > input[type=\"hidden\"]:nth-child(4)').type='block'")  # ciu_idx 노출
        # 640 x 414 -> ho
        try:
            driver.find_element_by_css_selector('#celeb_image_1').clear()
            driver.find_element_by_css_selector('#celeb_image_1').send_keys(
                '/Users/jieun/Desktop/업무/인물등록/아카이브/이미지등록전/' + str(day) + '/' + str(i) + '_sq.jpg')  # 640x414 I/O 작업 실행
        except:
            try:
                driver.find_element_by_css_selector('#celeb_image_1').send_keys(
                    '/Users/jieun/Desktop/업무/인물등록/아카이브/이미지등록저/ᆫ' + str(day) + '/' + str(i) + '_sq.png')  # 640x414 I/O 작업 실행
            except:
                driver.find_element_by_css_selector('#celeb_image_1').send_keys(
                    '/Users/jieun/Desktop/업무/인물등록/아카이브/이미지등록전/' + str(day) + '/' + str(i) + '_sq.jpeg')

        # 640*698 - name[i] + '698.jpg'
        driver.execute_script("document.getElementById('f_mode').type='block'")  # 작업 모드 Input Tag 노출
        driver.find_element_by_css_selector('#f_mode').send_keys('insert')  # insert = 삽입, delete = 삭제
        driver.execute_script("document.getElementById('celeb_image_2').style='display:block'")  # Image Input Tag 노출
        driver.execute_script(
            "document.querySelector('#update > input[type=\"hidden\"]:nth-child(1)').type='block'")  # cd_idx 노출
        driver.execute_script(
            "document.querySelector('#update > input[type=\"hidden\"]:nth-child(4)').type='block'")  # ciu_idx 노출
        # 640 x 698 -> ve
        try:
            driver.find_element_by_css_selector('#celeb_image_2').clear()
            driver.find_element_by_css_selector('#celeb_image_2').send_keys(
                '/Users/jieun/Desktop/업무/인물등록/아카이브/이미지등록전/' + str(day) + '/' + str(i) + '_sq.jpg')  # 640x698 I/O 작업 실행
        except:
            try:
                driver.find_element_by_css_selector('#celeb_image_2').send_keys(
                    '/Users/jieun/Desktop/업무/인물등록/아카이브/이미지등록전/' + str(day) + '/' + str(i) + '_sq.png')  # 640x698 I/O 작업 실행
            except:
                driver.find_element_by_css_selector('#celeb_image_2').send_keys(
                    '/Users/jieun/Desktop/업무/인물등록/아카이브/이미지등록전/' + str(day) + '/' + str(i) + '_sq.jpeg')
        time.sleep(2)

    except Exception as e :
        print("이미지 찾을 수 없음: ", i, ' - ', name)
        null = {'cd_idx': str(i), 'name': name}
        null_data.append(null)

nullDf = pd.DataFrame(null_data)
nullDf.to_excel("/Users/jieun/Desktop/업무/인물등록/null_data/null_image_list_"+date1+".xlsx", index=False)

driver.close()


