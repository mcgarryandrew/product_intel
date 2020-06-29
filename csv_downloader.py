 #!/bin/sh
import os
import env
import time
import glob
from datetime import date
from selenium import webdriver  
from selenium.webdriver.common.keys import Keys  
from selenium.webdriver.chrome.options import Options
def DownloadMerchantCSV(DL,NEW_NAME):
    headless = 1
    DOWNLOAD_PATH = DL
    chrome_options = Options()

    chrome_options.add_argument("--window-size=1920,1080")

    if headless == 1:
        chrome_options.add_argument('--headless')    
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--ignore-certificate-errors')
    driver = webdriver.Chrome(executable_path=os.path.abspath("/usr/bin/chromedriver"),   chrome_options=chrome_options)  
    
    driver.command_executor._commands["send_command"] = ("POST", '/session/$sessionId/chromium/send_command')
    params = {'cmd': 'Page.setDownloadBehavior', 'params': {'behavior': 'allow', 'downloadPath': DOWNLOAD_PATH}}
    command_result = driver.execute("send_command", params)
    
    username = os.getenv('EMAIL')
    password = os.getenv('PASSWORD')

    driver.get('https://accounts.google.com/signin/v2/identifier?service=merchants&passive=1209600&continue=https%3A%2F%2Fmerchants.google.com%2Fmc%2Fdefault%3Fhl%3Den%26fmp%3D1%26utm_id%3Dgfr%26mcsubid%3Duk-en-web-g-mc-gfr%26_ga%3D2.30098943.1769887343.1591033073-1156362069.1591033073&followup=https%3A%2F%2Fmerchants.google.com%2Fmc%2Fdefault%3Fhl%3Den%26fmp%3D1%26utm_id%3Dgfr%26mcsubid%3Duk-en-web-g-mc-gfr%26_ga%3D2.30098943.1769887343.1591033073-1156362069.1591033073&hl=en&flowName=GlifWebSignIn&flowEntry=ServiceLogin') #go to desired site and continue navigation
    driver.implicitly_wait(10)
    if headless == 1:
        driver.find_element_by_name('Email').send_keys(username)
        driver.find_element_by_name('signIn').click()
    else:
        driver.find_element_by_name('identifier').send_keys(username)
        driver.find_element_by_xpath('//*[@id="identifierNext"]/span/span').click()

    driver.implicitly_wait(20)
    if headless == 1:
        driver.find_element_by_name('Passwd').send_keys(password)
        driver.implicitly_wait(4)
        driver.find_element_by_id('submit').click()
    else:
        driver.find_element_by_name('password').send_keys(password)
        driver.implicitly_wait(4)
        driver.find_element_by_xpath('//*[@id="passwordNext"]/span/span').click()

    time.sleep(20)
    driver.get('https://merchants.google.com/mc/bestsellers?a='+ os.getenv('MERCHANT_ID') +'&tab=product&tableState=ChYKDGNvdW50cnlfY29kZRABGgQyAlVTCloKD3JhbmtlZF9jYXRlZ29yeRADGkU6Q3siMSI6IjE4NyIsIjIiOiJTaG9lcyIsIjMiOnsiMSI6IjE2NiIsIjIiOiJBcHBhcmVsICYgQWNjZXNzb3JpZXMifX0SGgoWZ29vZ2xlX3BvcHVsYXJpdHlfcmFuaxABGDI%3D&hl=en&fmp=1&utm_id=gfr&mcsubid=uk-en-web-g-mc-gfr&_ga=2.27780219.632847136.1591024293-1801482839.1591024293')
    time.sleep(20)
    driver.find_element_by_xpath('//*[@id="products-root"]/product-google-popularity-rank-view/best-sellers-tabs/scroll-host-with-footer/product-google-popularity-rank-report/tableview/toolbelt/div/toolbelt-bar[1]/div[2]/div[2]/element/icon-text/material-button/material-ripple').click()
    time.sleep(30) #wait for the download to finish
    #Download should be finished
    driver.close()
    
    CSV_NAME = ""
    for name in glob.glob(DOWNLOAD_PATH + "/*.csv"):
        CSV_NAME = name
    os.rename(CSV_NAME, os.path.join(DOWNLOAD_PATH, NEW_NAME))
    return os.path.join(DOWNLOAD_PATH, NEW_NAME)
