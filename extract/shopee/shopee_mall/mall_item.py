import json
import logging
import os
import pathlib
import random
from datetime import datetime
import requests
import time
import gspread
import pandas as pd
from seleniumwire import webdriver
from seleniumwire.utils import decode
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.proxy import Proxy, ProxyType
from pyvirtualdisplay import Display
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service

from helpers.cloud_storage_helper import upload_blob_to_gcs, download_folder_from_gcs, list_blob_gcs, download_blob_to_local
from helpers.time_helper import sleep_condition
from helpers.webdriver_helper import connect_to_vpn, connect_to_oxylab, get_random_user_agent

def get_shop():
    read_csv = pd.read_csv("./extract/shopee/shopee_mall/Shopee_mall_shop.csv")
    # Convert the column to integers
    read_csv['shop_id'] = read_csv['shop_id'].astype(int)
    return read_csv


# def set_webdriver(proxy_host=None):
#     profile = webdriver.FirefoxProfile("/app/.mozilla/firefox/v0oygky2.default")
#     options = Options()
#     options.set_preference("browser.download.folderList", 2)
#     options.set_preference("browser.download.manager.showWhenStarting", False)
#     options.set_preference("browser.download.dir", "/app/Downloads/")
#     options.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/x-gzip")

#     if proxy_host:
#         proxy = Proxy()
#         proxy.proxy_type = ProxyType.MANUAL
#         proxy.http_proxy = proxy_host
#         proxy.ssl_proxy = proxy_host
#         options.proxy = proxy

#     driver = webdriver.Firefox(
#         executable_path="/usr/local/bin/geckodriver",
#         firefox_profile=profile,
#         options=options
#     )

#     return driver

def set_webdriver():
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_experimental_option("excludeSwitches", ['enable-automation'])
    chrome_options.set_capability("browserVersion", "109")
    return webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=chrome_options
    )


def copy_profile(bucket_name, base_path):
    destination_path = "/app/.mozilla/firefox/v0oygky2.default"
    download_path = "/app/Downloads/"
    pathlib.Path(destination_path).mkdir(parents=True, exist_ok=True)
    pathlib.Path(download_path).mkdir(parents=True, exist_ok=True)

    list_profile = ["profile2", "profile3", "profile4"]
    profile_used = random.choice(list_profile)
    gcs_path = f"{base_path}/{profile_used}/"

    download_folder_from_gcs(bucket_name=bucket_name, gcs_path=gcs_path, destination_path=destination_path)
    print("finish copy profile")


def login(driver): 
    user = ["xxx@xxx.com", "xxx@xxx.com"]
    pwd  = "xxx"
    user = random.choice(user)

    # get element username
    username = driver.find_element(By.XPATH,"//input[@placeholder='No. Handphone/Username/Email']")
    username.send_keys(user)

    # get element password
    password = driver.find_element(By.XPATH,"//input[@placeholder='Password']")
    password.send_keys(pwd)
    sleep_condition(2,4)

    #login click
    login_click = driver.find_element(By.XPATH,"//button[normalize-space()='Log in']")
    login_click.click()
    



def extract_data(driver, page: int, shop_code: str, base_dir: str, bucket_name: str, base_path: str, run_date: str):
    # https://shopee.co.id/unileverindonesia?page=1&sortBy=pop
    # page = page + 'halaman'
    driver.get(f"https://shopee.co.id/{shop_code}?page={page}")
    sleep_condition(10, 30)
    
    # try: 
    #     login(driver)
    #     print("sukses login")
    #     sleep_condition(10, 30)
    # except: 
    #     print("no need login")
    
    request_url = "rcmd_items"

    current_directory = os.getcwd()
    print(current_directory)
    files = os.listdir(current_directory)
    print("Current directory files:", files)

    # Specify the file path
    json_filename = os.path.join(base_dir, f"{shop_code}_page_{str(page)}.json")

    for request in driver.requests:
        if request.response:
            if request.url.find(request_url) > 0:
                logging.info(request.url, request.response.status_code, request.response.headers['Content-Type'])
                body = decode(request.response.body, request.response.headers.get('Content-Encoding', 'identity'))
                # print(body)
                try:
                    with open(json_filename, 'w') as f:
                        json.dump(json.loads(body), f)
                    # print(f'finish get from shopee {json_filename}')
                except Exception as e:
                    print("Error json:", str(e))

    # check body value
    if 'body' not in locals() and 'body' not in globals():
        print(f'body doesnt exist and {shop_code} wont load')
    else:
        print('body has value')

    blob_name = f"{base_path}/{run_date}/{shop_code}_page_{str(page)}.json"
    upload_blob_to_gcs(bucket_name=bucket_name, local_file_name=json_filename, gcs_blob_name=blob_name)
    print(f"successfully write file to {blob_name}")


def get_api(shop_code: int, base_dir: str, shop_name: str, bucket_name: str, base_path: str, run_date: str, oxylab_use: str): 
    # url = f"https://shopee.co.id/api/v4/recommend/recommend?bundle=shop_page_product_tab_main&limit=999&offset=0&section=shop_page_product_tab_main_sec&shopid={str(shop_code)}"
    url = f"https://shopee.co.id/api/v4/shop/rcmd_items?bundle=shop_page_category_tab_main&limit=100&offset=0&shop_id={shop_code}&sort_type=1&upstream="
    print(url)

    if oxylab_use != "no": 
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36'}
        proxies = connect_to_oxylab()
        response = requests.get(url, headers=get_random_user_agent(), proxies=proxies)
    else: 
        response = requests.get(url, headers=get_random_user_agent())
        print("not using oxylab")


    if response.status_code == 200:
        time.sleep(3)
        json_data = response.json()
        headers = response.headers

    else:
        print("Failed to fetch JSON data:", response.status_code)
        pass

    # Convert the dictionary to a JSON-formatted string
    json_string = json.dumps(json_data)
    # Parse the JSON string into a Python dictionary
    data = json.loads(json_string)

    json_filename = f"{base_dir}/{shop_name}.json"
            
    with open(json_filename, 'w') as json_file:
        json.dump(data, json_file, indent=4)

    with open("headers.json", "w") as f:
        json.dump(headers, f, indent=4)

    blob_name = f"{base_path}/{run_date}/{shop_name}.json"
    upload_blob_to_gcs(bucket_name=bucket_name, local_file_name=json_filename, gcs_blob_name=blob_name)
    blob_name_headers = f"{base_path}/{run_date}/{shop_name}_header.json"
    upload_blob_to_gcs(bucket_name=bucket_name, local_file_name=json_filename, gcs_blob_name=blob_name_headers)
    logging.info(f"successfully write file to {blob_name}")

def choose_vpn(bucket_name): 
     #list vpn profile file in gcs
    vpn_profile_list = list_blob_gcs(bucket_name=bucket_name, prefix_name="assets/ovpn/")
    vpn_profile_list.remove('assets/ovpn/')
    picked_profile = random.choice(vpn_profile_list)
    print(f"the profile used is {picked_profile}")

     # download vpn profile from cloud storage
    vpn_filename = picked_profile.split('/')[-1]
    download_blob_to_local(bucket_name=bucket_name, local_file_name=vpn_filename, gcs_blob_name=picked_profile)
    print("finish download profile vpn")
    time.sleep(5)
    return vpn_filename


def transform_data(shop_name, bucket_name, base_path, table_name, schema_path, run_date):
    from transform.shopee.shopee_mall import mall_item_tf
    mall_item_tf.main(
        shop_name,
        bucket_name,
        base_path,
        table_name,
        schema_path,
        run_date,
    )


def main(start: int, end: int, bucket_name: str, base_path: str, run_date: str, target_table: str, schema_path: str, 
         shop_spec: str, vpn_use="no", oxylab_use="yes"):

    base_dir = os.getcwd() + f'/{base_path}/{run_date}'
    pathlib.Path(base_dir).mkdir(parents=True, exist_ok=True)
    print(f'the {base_dir} is exist now')

    print(f"the start value is {start}")
    # print(f"the retry count value is {retry_count}")


    read_csv_shop = get_shop()
    shop_id = list(read_csv_shop['shop_id'])
    random.shuffle(shop_id)
    print("we use normal list shop")

    # copy user login profile with firefox
    # copy_profile(bucket_name=bucket_name, base_path=base_path)

    # vpn_used = choose_vpn(bucket_name=bucket_name)

    # if vpn_use == "yes":
    #     connect_to_vpn(vpn_used)
    # sleep_condition(10, 20)

    print(shop_id)
    # disp = Display(size=(1920, 1080))
    # disp.start()
    # driver = set_webdriver()

    for ids in shop_id:
        print(f"processing {ids}")
        if ids == 0 : 
            pass 
        else: 
            shop_name = list(read_csv_shop[read_csv_shop['shop_id'] == ids]['shop_name'])[0]
            shop_code = list(read_csv_shop[read_csv_shop['shop_id'] == ids]['shop_code'])[0]
            print(shop_name)
            print("===============")
            print(shop_code)
            
            max_retries = 10
            retry_count = 0
            
            while retry_count < max_retries:
                try: 
                    get_api(ids, base_dir, shop_name, bucket_name, base_path, run_date, oxylab_use)
                    print(f"data from {shop_name} is already retreived")
                    transform_data(shop_name, bucket_name, base_path, target_table, schema_path, run_date)
                    # sleep_condition(1,2)
                    break
                except Exception as e : 
                    print(e)
                    retry_count += 1
                    if retry_count < max_retries:
                        print(f"retry attempt {retry_count}/{max_retries} for {ids}")
                    else: 
                        print(f"Category {ids}  won't load")
                    sleep_condition(10, 30)
                

        
        sleep_condition(10, 50)

    # driver.quit()
    # disp.stop()
