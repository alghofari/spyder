import json
import logging
import os
import pathlib
import random
from datetime import datetime
import requests

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

from helpers.cloud_storage_helper import upload_blob_to_gcs, download_folder_from_gcs
from helpers.time_helper import sleep_condition
from helpers.bigquery_helper import df_to_bq
from datetime import datetime

def get_shop():
    read_csv = pd.read_csv("./extract/shopee/shopee_mall/shopee_mall_shop_v2.csv")
    return read_csv


def set_webdriver(proxy_host=None):
    profile = webdriver.FirefoxProfile("/app/.mozilla/firefox/v0oygky2.default")
    # profile = webdriver.FirefoxProfile("/home/ffkhrsirclo/.mozilla/firefox/746ogt4m.mp_profile3")

    options = Options()
    options.set_preference("browser.download.folderList", 2)
    options.set_preference("browser.download.manager.showWhenStarting", False)
    options.set_preference("browser.download.dir", "/app/Downloads/")
    options.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/x-gzip")

    if proxy_host:
        proxy = Proxy()
        proxy.proxy_type = ProxyType.MANUAL
        proxy.http_proxy = proxy_host
        proxy.ssl_proxy = proxy_host
        options.proxy = proxy

    driver = webdriver.Firefox(
        executable_path="/usr/local/bin/geckodriver",
        firefox_profile=profile,
        options=options
    )

    return driver

def copy_profile(bucket_name, base_path):
    destination_path = "/app/.mozilla/firefox/v0oygky2.default"
    download_path = "/app//Downloads/"
    pathlib.Path(destination_path).mkdir(parents=True, exist_ok=True)
    pathlib.Path(download_path).mkdir(parents=True, exist_ok=True)

    list_profile = ["profile2", "profile3"]
    profile_used = random.choice(list_profile)
    gcs_path = f"{base_path}/{profile_used}/"

    download_folder_from_gcs(bucket_name=bucket_name, gcs_path=gcs_path, destination_path=destination_path)
    print("finish copy profile")

# Function to parsing json and convert it into pandas dataframe
def parsing_json(data: dict):
    new_df = pd.json_normalize(data)
    df_selected = new_df[["name", "item_count", "rating_star", "response_rate"]]
    return df_selected

def extract_data(driver, url: str, base_dir: str, bucket_name: str, base_path: str, run_date: str):
    # https://shopee.co.id/unileverindonesia?page=1&sortBy=pop
    # page = page + 'halaman'
    driver.get(url)
    sleep_condition(10, 30)
    
    request_url = "get_shop_base"

    current_directory = os.getcwd()
    print(current_directory)
    files = os.listdir(current_directory)
    print("Current directory files:", files)
    shop_code = url.split('/')[-1]

    # Specify the file path
    json_filename = os.path.join(base_dir, f"{shop_code}_page_os_info.json")

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
    
    # get the important part of json body
    body = json.loads(body)
    try: 
        result = body['data']
    except: 
        result = "nothing"
    return result

# Main Function
def main(bucket_name: str, base_path: str, run_date: str, table_name: str, schema_path: str):
    
    # check if directory exist, if not will create directory
    base_dir = os.getcwd() + f'/{base_path}/{run_date}'
    pathlib.Path(base_dir).mkdir(parents=True, exist_ok=True)

    # shuffling shop OS
    read_csv_shop = get_shop()
    brand_url = list(read_csv_shop['brand_url'])
    random.shuffle(brand_url)

    # copy user login profile with firefox
    copy_profile(bucket_name=bucket_name, base_path=base_path)

    disp = Display(size=(1920, 1080))
    disp.start()
    driver = set_webdriver()

    for i in range(len(brand_url)):

        url = brand_url[i]
        shop_code = url.split('/')[-1]
        data = extract_data(driver, url, base_dir, bucket_name, base_path, run_date)
        
        if data == "nothing": 
            print("no getting needed info")
            continue
        else:
            sleep_condition(5, 10)
            data_new = parsing_json(data)
            data_new.insert(0, "load_timestamp", datetime.now())
            print(data_new)

            # Load to bigquery
            print("finish parsing")
            try: 
                df_to_bq(data_new, table_name, schema_path, "load_timestamp", "mall_item")
                print(f"{shop_code} has been inserted to {table_name}")
            except Exception as e:
                logging.error(e)
                print("wrong schema for shop ", shop_code, e)
                continue
        
        print(f"Selesai mengerjakan shop ke {str(i)}")

        
    driver.quit()
    disp.stop()