import json
import logging
import os
import pathlib
import random
import requests
import time
from datetime import datetime
import pandas as pd

import gspread
from selenium.webdriver.chrome.service import Service
from seleniumwire import webdriver
from seleniumwire.utils import decode
from webdriver_manager.chrome import ChromeDriverManager

from helpers.cloud_storage_helper import upload_blob_to_gcs
from helpers.time_helper import sleep_condition
from helpers.webdriver_helper import connect_to_oxylab, get_random_user_agent


def get_random_category(col1: str, col2: str): 

    # get the table
    df = pd.read_csv("./extract/shopee/shopee_item/Shopee_category_v2.csv")

    # Concatenate & randomize the two columns into a new column 'combined'
    df['combined'] = df[col1].astype(str) + '_' + df[col2].astype(str)
    df_randomized = df.sample(frac=1).reset_index(drop=True)

    # Split the 'combined' column back into the original two columns & drop temp column
    df_randomized[[col1, col2]] = df_randomized['combined'].str.split('_', expand=True)
    df_randomized = df_randomized.drop(columns=['combined'])
    return df_randomized


def get_api(id_category: str, base_dir:str, bucket_name: str, base_path, run_date: str, category: str, oxylab_use: str): #page: str
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36'}
    url_with_code = f"https://shopee.co.id/api/v4/recommend/recommend?bundle=category_landing_page&cat_level=1&catid={id_category}&limit=240&offset=0"
    
    if oxylab_use != "no": 
        proxies = connect_to_oxylab()
        response = requests.get(url_with_code, headers=get_random_user_agent(), proxies=proxies) 
    else: 
        response = requests.get(url_with_code, headers=get_random_user_agent())
        print("not using oxylab")
    
    response = requests.get(url_with_code, headers=get_random_user_agent())
    print(url_with_code)

    if response.status_code == 200:
        time.sleep(3)
        json_data = response.json()
        headers = response.headers

    else:
        print("Failed to fetch JSON data:", response.status_code)
        pass

    # Convert the dictionary to a JSON-formatted string
    json_string = json.dumps(json_data)
    json_filename = f"{base_dir}/{category}_page_1.json" #{str(page)}
    with open(json_filename, 'w') as f:
        json.dump(json_data, f)
    
    with open("headers.json", "w") as f:
        json.dump(headers, f, indent=4)
    
    blob_name = f"{base_path}/{run_date}/{category}_page_1.json" #{str(page)}
    upload_blob_to_gcs(bucket_name=bucket_name, local_file_name=json_filename, gcs_blob_name=blob_name)
    blob_name_headers = f"{base_path}/{run_date}/{category}_header.json"
    upload_blob_to_gcs(bucket_name=bucket_name, local_file_name=json_filename, gcs_blob_name=blob_name_headers)
    
    logging.info(f"successfully write file to {blob_name}")


def set_webdriver():
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_experimental_option("excludeSwitches", ['enable-automation'])
    chrome_options.set_capability("browserVersion", "109")
    
    # proxy_ip = connect_to_oxylab()
    # chrome_options.add_argument(f"--proxy-server={proxy_ip}")
    return webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=chrome_options
    )


def extract_data(driver, page: int, category: str, base_dir: str, bucket_name: str, base_path: str, sort_by: str, run_date: str):
    driver.get(f"https://shopee.co.id/{category}?page={str(page)}")
    sleep_condition(10, 20)

    request_url = "recommend"
    json_filename = f"{base_dir}/{category}_page_{str(page)}.json"

    for request in driver.requests:
        if request.response:
            if request.url.find(request_url) > 0:
                logging.info(request.url, request.response.status_code, request.response.headers['Content-Type'])
                body = decode(request.response.body, request.response.headers.get('Content-Encoding', 'identity'))
                with open(json_filename, 'w') as f:
                    json.dump(json.loads(body), f)

    blob_name = f"{base_path}/{run_date}/{category}_page_{str(page)}.json"
    upload_blob_to_gcs(bucket_name=bucket_name, local_file_name=json_filename, gcs_blob_name=blob_name)
    logging.info(f"successfully write file to {blob_name}")


def transform_data(category_name, page, bucket_name, base_path, target_table, schema_path, sort_by):
    from transform.shopee.search_item import transform_item
    transform_item.main(
        category_name,
        page,
        bucket_name,
        base_path,
        target_table,
        datetime.now(),
        schema_path,
        sort_by
    )

# start: int, end: int, 
def main(bucket_name: str, base_path: str, sort_by: str, run_date: str, target_table: str, schema_path: str, oxylab_use="yes"):

    base_dir = os.getcwd() + f'/{base_path}/{run_date}'
    pathlib.Path(base_dir).mkdir(parents=True, exist_ok=True)

    df_category = get_random_category("Category Name","category_code")
    categories = list(df_category['Category Name'])

    # driver = set_webdriver()
    print(categories)
    

    for category in categories:
        category_code = list(df_category[df_category['Category Name'] == category]['category_code'])[0]

        # for page in range(start, end):
        #     print(f"Doing page number {page}")

        max_retries = 10
        retry = 0
        while retry < max_retries:
            try:
                # extract_data(driver, page, category, base_dir, bucket_name, base_path, sort_by, run_date)
                get_api(category_code, base_dir, bucket_name, base_path, run_date, category, oxylab_use)
                print("get the data")
                sleep_condition(1,2)
                transform_data(category, "1", bucket_name, base_path, target_table, schema_path, sort_by)
                break
            except Exception as e:
                print(e)
                retry += 1
                if retry < max_retries:
                        sleep_condition(5, 15)
                        print(f"retry attempt {retry}/{max_retries} for {category}")
                else: 
                    print(f"Category {category} won't load")
                sleep_condition(3, 6)
                time.sleep(5)
                

        sleep_condition(10, 30)
    # driver.quit()
