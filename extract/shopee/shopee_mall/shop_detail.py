import json
import logging
import os
import pathlib
import time
from seleniumwire.utils import decode
from google.cloud import storage, bigquery
from helpers.cloud_storage_helper import upload_blob_to_gcs
from helpers.bigquery_helper import bq_to_df
from helpers.webdriver_helper import set_webdriver
client = bigquery.Client()

def get_category(category: str, level=1):
    return category.rsplit('/', 2)[-level]

def get_data_to_gcs(driver, url: str, base_dir: str, bucket_name: str, base_path: str, run_date: str):
    driver.get(url)
    time.sleep(10)
    store_name = get_category(url)
    json_filename = f"{base_dir}/{store_name}.json"
    print(f"getting info from {url}")
    for request in driver.requests:
        if request.response:
            if request.url.find('get_shop_base') > 0:
                logging.info(request.url, request.response.status_code, request.response.headers['Content-Type'])
                body = decode(request.response.body, request.response.headers.get('Content-Encoding', 'identity'))
                with open(json_filename, 'w') as f:
                    json.dump(json.loads(body), f)
    blob_name = f"{base_path}/{run_date}/{store_name}.json"
    upload_blob_to_gcs(bucket_name=bucket_name, local_file_name=json_filename, gcs_blob_name=blob_name)
    print(f"successfully write file to {blob_name}")

def main(bucket_name: str, base_path: str, query: str, run_date: str):
    opts = ['--headless']
    driver = set_webdriver(additional_opts=opts, driver_type='seleniumwire')
    base_dir = os.getcwd() + f'/{base_path}/{run_date}'
    pathlib.Path(base_dir).mkdir(parents=True, exist_ok=True)
    query_result = bq_to_df(query)
    for url in query_result["brand_url"]:
        try:
            get_data_to_gcs(driver=driver, url=url, base_dir=base_dir, bucket_name=bucket_name,
                     base_path=base_path, run_date=run_date)
            print("finish")
        except Exception as e:
            print(f"Error pada halaman toko: {e}")
    driver.quit()