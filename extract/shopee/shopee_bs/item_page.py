import json
import os
import pathlib
import random
import time
import logging

import pandas as pd
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from pyvirtualdisplay import Display

from helpers.cloud_storage_helper import download_blob_to_local, list_blob_gcs, upload_blob_to_gcs
from helpers.webdriver_helper import set_webdriver
from transform.shopee.shopee_bs.processing import Processing

p = Processing()


# getting files from gcs
def download_from_gcs(category: str, base_dir: str, run_date: str, bucket_name: str, base_path: str):
    list_blob = list_blob_gcs(bucket_name, f"{base_path}/category/{run_date}/{category}")
    print(f"checking blob file at storage path /{base_path}/category/{run_date}")
    for index, file in enumerate(list_blob):
        print(f"list file in blob gcs: {file}")
        file_name = f'{base_dir}/{category}_page_{str(index)}.html'
        download_blob_to_local(bucket_name=bucket_name, local_file_name=file_name, gcs_blob_name=file)
        print(f'download finish for {category}{index}')


# clean html file to get list of url
def cleansing_html(file_html: str, base_dir: str):
    read_html = open(f'{base_dir}/{file_html}', 'r')
    soup = BeautifulSoup(read_html, 'html.parser')
    res = soup.find_all('script', type='application/ld+json')

    # get data from application/ld+json
    all_data = {}
    x = 1
    for i in range(len(res)):
        for data in res[i]:
            json_object = json.loads(data)
            all_data[f'df_{x}'] = pd.json_normalize(json_object)
            x = x + 1
            
    list_key = [all_data[i] for i in all_data]
    try:
        df_1 = pd.concat(list_key, ignore_index=True)
        list_url = list(df_1['url'])
    except Exception as e:
        print(e)
        print("no data")
        return

    return list_url


def get_data(page: int, file_html: str, category: str, base_dir: str, bucket_name: str, base_path: str, run_date: str,
             short_slp: int, long_slp: int):
    # opts = ['--headless']
    print(f"checking list of url on {file_html}")
    get_url = cleansing_html(file_html=file_html, base_dir=base_dir)
    get_url = [url for url in get_url if str(url) != 'nan' or url is not None]
    if len(get_url) < 0:
        return
    else:
        print(f"checking list of url : {get_url}")
        for index, url in enumerate(get_url):
            disp = Display(size=(1920, 1080))
            disp.start()
            logging.info("Opening virtual display")
            driver = set_webdriver()
            try:
                driver.get(url)
                time.sleep(3)
                print('getting sleep 3 sec')
                html = driver.page_source

                html_filename = f"{base_dir}/{category}_item_{str(page)}_{index}.html"
                with open(html_filename, "w") as f:
                    f.write(html)

                blob_name = f"{base_path}/item/{run_date}/{category}_item_{str(page)}_{index}.html"
                upload_blob_to_gcs(bucket_name=bucket_name, local_file_name=html_filename, gcs_blob_name=blob_name)
                print(f"successfully write {html_filename}")

            except Exception as e:
                print(e)
                print(f"{url} cannot be opened")
                continue
            #closing the driver
            driver.quit()
            disp.stop()
            logging.info("Closing virtual display")
            time.sleep(random.uniform(short_slp, long_slp))

def main(category: str, bucket_name: str, base_path: str, run_date: str, short_slp: int, long_slp: int):
    base_dir = os.getcwd() + f'/assets/html/shopee/category/{run_date}'
    pathlib.Path(base_dir).mkdir(parents=True, exist_ok=True)

    download_from_gcs(category=category,
                      base_dir=base_dir,
                      run_date=run_date,
                      bucket_name=bucket_name,
                      base_path=base_path)

    filtered_html = [i for i in os.listdir(base_dir) if category in i and 'page' in i]
    print(f"checking html file at storage {filtered_html}")
    num = 0
    for html_file in filtered_html:
        # cleansing_html(file_html=html_file,page=num,base_dir=base_dir)
        try:
            get_data(page=num, file_html=html_file, category=category, base_dir=base_dir, bucket_name=bucket_name,
                     base_path=base_path, run_date=run_date, short_slp=short_slp, long_slp=long_slp)
        except Exception as e:
            print(e)
            print("html file is not sufficient")
            continue

        print("finish")
        num += 1
