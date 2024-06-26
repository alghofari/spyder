import json
import logging
import os
import pathlib
import random
from datetime import datetime
import requests

import gspread
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from seleniumwire import webdriver
from seleniumwire.utils import decode
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from seleniumwire import webdriver as webdriver_sl

from helpers.cloud_storage_helper import upload_blob_to_gcs
from helpers.time_helper import sleep_condition

# Note using this code 
# 1. change user n password of shopee account
# 2. previously still running separetly between extract and transform. Need to check it before RUN


def get_shop():
    google_application_credentials = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
    gc = gspread.service_account(filename=google_application_credentials)
    sh = gc.open_by_key('1jvrwFwpunb25RYeRxVtg_dZqrF8o-XVmByKwl-LFv5A')
    worksheet = sh.worksheet("Shopee mall")
    return worksheet.col_values(1)[1:]

def set_webdriver():

    options = Options()

    default_opts = [
        '--no-sandbox',
        '--disable-dev-shm-usage',
        '--disable-blink-features=AutomationControlled',
        '--ignore-certificate-errors',
        '--ignore-ssl-errors=yes',
        '--allow-insecure-localhost',
        'window-size=1920,1080',
        'disable-infobars',
        'user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
    ]

    for default_opt in default_opts:
        options.add_argument(default_opt)
    
    caps = options.to_capabilities()
    caps["acceptInsecureCerts"] = True

    chrome_options_sw = webdriver_sl.ChromeOptions()
    for opt in default_opts:
        chrome_options_sw.add_argument(opt)

    driver = webdriver_sl.Chrome(
                service=Service(ChromeDriverManager().install()), 
                options=chrome_options_sw, 
                desired_capabilities=caps
                )
    return driver

def login(driver): 
    # get element username
    username = driver.find_element(By.XPATH,"//input[@placeholder='No. Handphone/Username/Email']")
    username.send_keys("notelpon")

    # get element password
    password = driver.find_element(By.XPATH,"//input[@placeholder='Password']")
    password.send_keys("password!")
    sleep_condition(2,4)

    #login click
    login_click = driver.find_element(By.XPATH,"//button[normalize-space()='Log in']")
    login_click.click()


def extract_data(driver, page: int, shop_name: str):
    # https://shopee.co.id/unileverindonesia?page=1&sortBy=pop
    driver = set_webdriver()
    driver.get(f"https://shopee.co.id/{shop_name}?page={page}&sortBy=pop")
    sleep_condition(3, 8)

    # Find all the desired elements using XPath
    elements = driver.find_elements(By.XPATH,'//div[@class="row"]//div[@class="shop-search-result-view__item col-xs-2-4"]/a[@data-sqe="link"]')
    
    # Extract the href values
    href_list = [element.get_attribute("href") for element in elements]
    href_list = [page.split('?sp_atk')[0] for page in href_list]
    print(href_list)
    print(f"get list of url from page {str(page)} of {shop_name}")
    driver.quit()

    for index,href in enumerate(href_list):
        success = True # Flag variable to track success
        driver = set_webdriver()
        try: 
            driver.get(href)
            print(f"open page {href}")
            sleep_condition(5, 15)
            login(driver)
            sleep_condition(10, 20)
        except:
            success = False  # Update the flag variable
            print(f"{href} not opened")

        if success: 
            #scrolling into review
            for i in range(5):
                driver.execute_script("window.scrollBy(0,200)")
                sleep_condition(1,2)

            parent_star = driver.find_element(By.XPATH,"//div[@class='product-rating-overview__filters']")
            stars_button = parent_star.find_elements(By.XPATH,".//div[@class='product-rating-overview__filter']")
            stars_text = [star.text for star in stars_button]
            stars_text = [s for s in stars_text if 'Bintang' in s]
            stars_text_lower = [s[:2] + s[2].lower() + s[3:] for s in stars_text]

            #click the star
            for star in stars_text_lower: 
                star_element = driver.find_element(By.XPATH, f"//div[normalize-space()='{star}']")
                star_element.click()
                print(f'{star} sukses di klik pada item ke-{index} pada shop {shop_name} pada page item ke {str(page)}')
                sleep_condition(2,6)
                get_json(driver, star[0], 1, shop_name, index, page)
                for page_comment in range(2,11): 
                    try: 
                        page_element = driver.find_element(By.XPATH, f"//button[normalize-space()='{str(page_comment)}']")
                        page_element.click()
                        print(f'{star} sukses di klik pada item ke-{index} pada shop {shop_name} di page comment ke {str(page_comment)} page item ke {str(page)}')
                        get_json(driver, star[0], page_comment, shop_name, index, page)
                        sleep_condition(2,6)
                    except: 
                        print(f"page ke {str(page_comment)} tidak tersedia pada item ke-{index} pada shop {shop_name}")
                        sleep_condition(2,6)

            sleep_condition(2,10)
            driver.quit()

def get_json(driver, star_num: int, page: int, shop_name: str, item_index: int, page_item: int): 
    for request in driver.requests:
        if request.response:
            json_filename = f"json/sampling/{shop_name}/page{str(page_item+1)}/star_{str(star_num)}_item_{str(item_index)}_page_{str(page)}.json"
            if request.url.find('get_ratings') > 0 :
                logging.info(request.url, request.response.status_code, request.response.headers['Content-Type'])
                body = decode(request.response.body, request.response.headers.get('Content-Encoding', 'identity'))
                # print(body)
                with open(json_filename, 'w') as f:
                    json.dump(json.loads(body), f)
    print(f"sukses write json {json_filename}")

def transform_data(base_path, bucket_name, shop, table_name, schema_path):
    from transform.shopee.shopee_mall import mall_review_tf
    mall_review_tf.main(
        base_path,
        bucket_name,
        shop, 
        table_name, 
        schema_path
    )



def main(start: int, end: int, base_path: str, bucket_name: str, table_name: str, schema_path: str):
    # shops = get_shop()
    shops = ['unileverindonesia', 'mondelezofficial']
    driver = set_webdriver()
    print(shops)

    for shop in shops:
        # for page in range(start, end):
        #     extract_data(driver, page, shop)
        transform_data(base_path, bucket_name, shop, table_name, schema_path)
        print("=================")