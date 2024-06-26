import json
import logging
import os
import pathlib
import time

from seleniumwire.utils import decode
from google.cloud import bigquery
from helpers.webdriver_helper import set_webdriver
from helpers.bigquery_helper import df_to_bq

import pandas as pd

def get_data(driver, base_dir: str, target_table: str, schema_path: str):
    driver.get("https://shopee.co.id/mall/brands")
    time.sleep(10)

    # Authenticate the client
    bigquery_client = bigquery.Client()

    json_filename = f"{base_dir}/all_shop_info.json"

    for request in driver.requests:
        if request.response:
            if request.url.find('get_shops_by_category') > 0:
                logging.info(request.url, request.response.status_code, request.response.headers['Content-Type'])
                body = decode(request.response.body, request.response.headers.get('Content-Encoding', 'identity'))
                with open(json_filename, 'w') as f:
                    json.dump(json.loads(body), f)

    print("finish get data")
    # Open the JSON file and read the contents
    with open(json_filename) as f:
        data = json.load(f)

    #Parsing json 
    dfs = []
    data_brands = data['data']['brands']
    for word in range(len(data_brands)) : 
        brand_info = data_brands[word]['brand_ids']
        for brand in brand_info : 
            data_df = pd.DataFrame.from_dict(brand, orient='index').T
            dfs.append(data_df)
    
    print("finish parse data")

    # Concatenate the DataFrames into a single table
    table = pd.concat(dfs, ignore_index=True)
    table['brand_url'] = "https://shopee.co.id/" + table['username']
    print("finish add brand_url")

    df_to_bq(df=table, project_table_bq=target_table, file_path=schema_path, job_configuration="shop_info")

    print(f"successfully write file to bq")

def main(base_path: str, run_date: str, target_table:str, schema_path: str):
    opts = ['--headless']
    driver = set_webdriver(additional_opts=opts, driver_type='seleniumwire')
    base_dir = os.getcwd() + f'/{base_path}/{run_date}'
    pathlib.Path(base_dir).mkdir(parents=True, exist_ok=True)

    try:
        get_data(driver, base_dir, target_table, schema_path)
    except Exception as e:
        print(e)
        print("json file is not sufficient")

    driver.quit()