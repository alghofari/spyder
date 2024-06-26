import time
import os
import pandas as pd
from bs4 import BeautifulSoup
from helpers.bigquery_helper import df_to_bq
from helpers.webdriver_helper import set_webdriver
from helpers.cloud_storage_helper import upload_blob_to_gcs
import json
import pathlib
import requests
import logging


def find_string(html_item, class_, tag, tag_identifier="class"):
    value = html_item.find(tag, {tag_identifier: class_}).get_text()
    return value

def main(page: int, bucket_name: str, base_path: str, run_date: str, timestamp, table_name: str, schema_path: str):
    #response = requests.get("http://10.103.255.45:8000/indo")
    #response_dict = response.json()
    #list_proxy = response_dict["address"]
    base_dir = os.getcwd() + f'/{base_path}/{run_date}'
    pathlib.Path(base_dir).mkdir(parents=True, exist_ok=True)
    opts = ['--headless']
    driver = set_webdriver(additional_opts=opts, driver_type="webdriver")
    names = []
    links = []
    logos = []
    columns = ['store_name', 'store_link', 'store_logo']
    for j in range(page):
        print(f"Doing page {j + 1}")
        try:
            driver.get(f"https://www.tokopedia.com/official-store/brand/?page={j + 1}")

        except Exception as e:
            print(e)
            logging.error(e)
            continue
        time.sleep(3)
        for i in range(20):
            driver.execute_script("window.scrollBy(0,250)")
        driver.execute_script("window.scrollBy(0,50)")

        read_html = driver.page_source
        html = BeautifulSoup(read_html, 'html.parser')
        filename = f"{base_dir}/official_store_{str(j+1)}.html"
        with open(filename, "w", encoding="utf-8") as f:
            f.write(read_html)
        blob_name = f"{base_path}/{run_date}/official_store_{str(j+1)}.html"
        upload_blob_to_gcs(bucket_name=bucket_name, local_file_name=filename, gcs_blob_name=blob_name)

        stores = html.findAll('a', {'data-testid': 'lblOSBrandLink'}, href=True)
        for store in stores:
            try:
                link = store['href']
            except:
                link = None
            try:
                name = store.find('p', {'data-testid': 'lblOSBrBrandName'}).get_text()
            except:
                name = None
            try:
                logo = store.find('img', {'data-testid': 'imgOSBrBrandLogo'})['src']
            except:
                logo = None
            names.append(name)
            links.append(link)
            logos.append(logo)
    try:
        df = pd.DataFrame(columns=columns)
        df['store_name'] = names
        df['store_link'] = links
        df['store_logo'] = logos
        df.insert(0, 'load_timestamp', timestamp)
        df_to_bq(df, table_name, schema_path, 'load_timestamp', 'search_item')
    except Exception as e:
        print(e)
