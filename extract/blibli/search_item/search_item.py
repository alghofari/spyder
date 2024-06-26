import json
import logging
import os
import pathlib
import random
from datetime import datetime
import pandas as pd
import requests
import time
from bs4 import BeautifulSoup
import gspread
from selenium.webdriver.chrome.service import Service
from seleniumwire import webdriver
from seleniumwire.utils import decode
from webdriver_manager.chrome import ChromeDriverManager
from helpers.cloud_storage_helper import upload_blob_to_gcs, list_blob_gcs, download_blob_to_local
from helpers.time_helper import sleep_condition
from transform.blibli.search_item import transform_item
from helpers.webdriver_helper import connect_to_vpn

# def set_webdriver():
#     chrome_options = webdriver.ChromeOptions()
#     chrome_options.add_argument("--headless")
#     chrome_options.add_argument("--disable-dev-shm-usage")
#     chrome_options.add_argument("--no-sandbox")
#     chrome_options.add_experimental_option("excludeSwitches", ['enable-automation'])
#     chrome_options.set_capability("browserVersion", "109")
#     return webdriver.Chrome(
#         service=Service(ChromeDriverManager().install()),
#         #"/root/sirclo/chromedriver",
#         options=chrome_options
#     )


def set_webdriver_locally(proxy_ip=''):
    my_user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument(f"--user-agent={my_user_agent}")
    if proxy_ip != '':
        chrome_options.add_argument(f"--proxy-server={proxy_ip}")
    chrome_options.add_experimental_option("excludeSwitches", ['enable-automation'])
    chrome_options.set_capability("browserVersion", "109")
    return webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=chrome_options
    )


def choose_vpn_filename():
    bucket = 'sirclo-data-marketplace'
    prefix = 'assets/ovpn/'
    vpn_profile_list = list_blob_gcs(bucket_name=bucket, prefix_name=prefix)
    vpn_profile_list.remove(prefix)

    picked_profile = random.choice(vpn_profile_list)
    print(f"the profile used is {picked_profile}")

    # download vpn profile from cloud storage
    vpn_filename = picked_profile.split('/')[-1]
    download_blob_to_local(bucket_name=bucket, local_file_name=vpn_filename, gcs_blob_name=picked_profile)
    time.sleep(5)
    return vpn_filename


def extract_data(driver, page: int, category: str, base_dir: str, bucket_name: str, base_path: str, run_date: str, vpn_use: str, oxylab_use: str):
    start = page*40
    page_start = page+1
    print(f'page {page} for category {category}...')
    url = f"https://www.blibli.com/backend/search/products?category={category}&channelId=mobile-web&isMobileBCA=false&isOnlyPaginationCall=true&showFacet=false&page={str(page_start)}&start={str(start)}&intent=false"
    json_filename = f"{base_dir}/{category}_page_{str(page)}.json"

    if vpn_use == "yes":
        vpn_filename = choose_vpn_filename()
        connect_to_vpn(vpn_filename)

    try:
        driver.get(url)
        sleep_condition(2, 5)
        # Extract JSON data from page source using BeautifulSoup
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        json_data = soup.get_text()
        data = json.loads(json_data)
        with open(json_filename, 'w') as json_file:
            json.dump(data, json_file, indent=4)
        blob_name = f"{base_path}/{run_date}/{category}_page_{str(page)}.json"
        upload_blob_to_gcs(bucket_name=bucket_name, local_file_name=json_filename, gcs_blob_name=blob_name)
        logging.info(f"successfully write file to {blob_name}")
    except Exception as e:
        logging.error(f"Failed extracting URL json  {json_data} because of {e}")





def transform_data(category_name, page, bucket_name, base_path, target_table, schema_path):
    transform_item.main(
        category_name,
        page,
        bucket_name,
        base_path,
        target_table,
        datetime.now(),
        schema_path
    )

def get_public_ip():
    try:
        response = requests.get('https://api64.ipify.org?format=json')  # Using ipify API
        if response.status_code == 200:
            return response.json()['ip']
        else:
            return "Unable to fetch IP address."
    except Exception as e:
        return str(e)


def main(start: int, end: int, bucket_name: str, base_path: str, run_date: str, target_table: str, schema_path: str, vpn_use: str = 'no', oxylab_use: str = 'no'):

    base_dir = os.getcwd() + f'/{base_path}/{run_date}'
    pathlib.Path(base_dir).mkdir(parents=True, exist_ok=True)
    
    read_csv = pd.read_csv("./extract/blibli/search_item/blibli_category.csv")
    categories = read_csv['Category_Id']   
    random.shuffle(categories)

    driver = set_webdriver_locally()

    for i in range(len(categories)):
        category = categories[i]

        for page in range(start, end):
            logging.info(f"Doing category {category} page number {page} using locally set up webdriver")
            try:
                extract_data(driver, page, category, base_dir, bucket_name, base_path, run_date, vpn_use, oxylab_use)
            except Exception as e:
                logging.error("Failed using initial webdriver")
            sleep_condition(5, 10)
            logging.info("extract done")
            transform_data(category, page, bucket_name, base_path, target_table, schema_path)
            logging.info("transform and insert done")

    driver.quit()
