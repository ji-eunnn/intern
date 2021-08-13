from selenium import webdriver
from tqdm import tqdm
import time
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd


file = 'image_resize_list.xlsx'
pro_excel = pd.read_excel('/Users/jieun/Desktop/업무/인물등록/'+ file)
cd_idx = pro_excel['cd_idx']

#스타 등록하기
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless')
driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)


pid = 'mycelebsTempUser'
ppw = 'mycelebs!@rookie'
driver.get(f'http://{pid}:{ppw}@dev.mycelebs.com/donut/')
# alert = driver.switch_to_alert()
alert = driver.switch_to.alert
alert.accept()
time.sleep(2.0)

driver.find_element_by_css_selector('#adminId').send_keys('rookie')
driver.find_element_by_css_selector('#adminPw').send_keys('1234')
btn = driver.find_element_by_css_selector('#loginForm > button')
btn.click()

for i in tqdm(cd_idx):
    imageUrl = 'http://dev.mycelebs.com/donut/Celeb/ShowManageCeleb/' + str(i)
    driver.get(imageUrl)
    print(i)

    
    # profile_url_main
    profile_url_main = f'https://all.image.mycelebs.com/{i}/{i}_1125@1464.jpg' 
    try:
        driver.find_element_by_xpath('//*[@id="s_profile_url_main"]').clear()
        driver.find_element_by_xpath('//*[@id="s_profile_url_main"]').send_keys(profile_url_main)
    except Exception as e:
        pass
    
    # profile_url_rectangle_small
    profile_url_rectangle_small = f'https://all.image.mycelebs.com/{i}/{i}_1035@420.jpg' 
    try:
        driver.find_element_by_xpath('//*[@id="s_profile_url_rectangle_small"]').clear()
        driver.find_element_by_xpath('//*[@id="s_profile_url_rectangle_small"]').send_keys(profile_url_rectangle_small)
    except Exception as e:
        pass
    
    # profile_url_diagonal
    profile_url_diagonal = f'https://all.image.mycelebs.com/{i}/{i}_573@372.jpg' 
    try:
        driver.find_element_by_xpath('//*[@id="s_profile_url_diagonal"]').clear()
        driver.find_element_by_xpath('//*[@id="s_profile_url_diagonal"]').send_keys(profile_url_diagonal)
    except Exception as e:
        pass
    
    # profile_url_square_medium
    profile_url_square_medium = f'https://all.image.mycelebs.com/{i}/{i}_288@288.jpg' 
    try:
        driver.find_element_by_xpath('//*[@id="s_profile_url_square_medium"]').clear()
        driver.find_element_by_xpath('//*[@id="s_profile_url_square_medium"]').send_keys(profile_url_square_medium)
    except Exception as e:
        pass
    
    # profile_url_square_small
    profile_url_square_small = f'https://all.image.mycelebs.com/{i}/{i}_240@240.jpg' 
    try:
        driver.find_element_by_xpath('//*[@id="s_profile_url_square_small"]').clear()
        driver.find_element_by_xpath('//*[@id="s_profile_url_square_small"]').send_keys(profile_url_square_small)
    except Exception as e:
        pass
    
    # profile_url_round_large!
    profile_url_round_large = f'https://all.image.mycelebs.com/{i}/{i}_249@249.jpg' 
    try:
        driver.find_element_by_xpath('//*[@id="s_profile_url_round_large"]').clear()
        driver.find_element_by_xpath('//*[@id="s_profile_url_round_large"]').send_keys(profile_url_round_large)
    except Exception as e:
        pass
    
    # profile_url_round_medium
    profile_url_round_medium = f'https://all.image.mycelebs.com/{i}/{i}_144@144.jpg' 
    try:
        driver.find_element_by_xpath('//*[@id="s_profile_url_round_medium"]').clear()
        driver.find_element_by_xpath('//*[@id="s_profile_url_round_medium"]').send_keys(profile_url_round_medium)
    except Exception as e:
        pass
    
    # profile_url_round_small
    profile_url_round_small = f'https://all.image.mycelebs.com/{i}/{i}_108@108.jpg' 
    try:
        driver.find_element_by_xpath('//*[@id="s_profile_url_round_small"]').clear()
        driver.find_element_by_xpath('//*[@id="s_profile_url_round_small"]').send_keys(profile_url_round_small)
    except Exception as e:
        pass
    
    # profile_url_rectangle_large
    profile_url_rectangle_large = f'https://all.image.mycelebs.com/{i}/{i}_1035@738.jpg' 
    try:
        driver.find_element_by_xpath('//*[@id="s_profile_url_rectangle_large"]').clear()
        driver.find_element_by_xpath('//*[@id="s_profile_url_rectangle_large"]').send_keys(profile_url_rectangle_large)
    except Exception as e:
        pass
    
    # profile_url_square_large
    profile_url_square_large = f'https://all.image.mycelebs.com/{i}/{i}_420@420.jpg' 
    try:
        driver.find_element_by_xpath('//*[@id="s_profile_url_square_large"]').clear()
        driver.find_element_by_xpath('//*[@id="s_profile_url_square_large"]').send_keys(profile_url_square_large)
    except Exception as e:
        pass
    
    # 추가버튼
    driver.find_element_by_xpath('//*[@id="write"]/div/div/div[2]/div/input').click()
    # 정상처리 되었습니다.
    time.sleep(1)
    alert.accept()
    
driver.close()
    
