import json
import logging
import os
import pathlib
import random
from datetime import datetime
import pandas as pd

import gspread
from selenium.webdriver.chrome.service import Service
from seleniumwire import webdriver
from seleniumwire.utils import decode
from webdriver_manager.chrome import ChromeDriverManager

from helpers.cloud_storage_helper import upload_blob_to_gcs
from helpers.time_helper import sleep_condition


def get_categories():
    read_csv = pd.read_csv("/app/extract/shopee/shopee_item/Shopee_priority.csv")
    category      = read_csv['Category Name']
    name_category = read_csv['Real Category Name']
    return read_csv, list(category), list(name_category)


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


def extract_data(driver, page: int, category: str, df_category, base_dir: str, bucket_name: str, base_path: str, sort_by: str, run_date: str):
    # https://shopee.co.id/Suplemen-Makanan-cat.11043279.11043280?facet=100006&page=1

    unique_code = df_category[df_category['Real Category Name'] == category]['Unic Code'][0]
    driver.get(f"https://shopee.co.id/Suplemen-Makanan-cat.11043279.11043281?facet={str(unique_code)}&page={str(page)}")
    sleep_condition(10, 20)

    if sort_by == "sales":
        request_url = "search_items"
        top_sales_button = driver.find_element("xpath", '//*[@id="main"]/div/div[2]/div/div/div[3]/div[2]/div/div[1]/div[1]/div[3]')
        top_sales_button.click()
        sleep_condition(10, 20)
    elif sort_by == "latest":
        request_url = "search_items"
        latest_button = driver.find_element("xpath", '//*[@id="main"]/div/div[2]/div/div/div[3]/div[2]/div/div[1]/div[1]/div[2]')
        latest_button.click()
        sleep_condition(10, 20)
    else:
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


def main(start: int, end: int, bucket_name: str, base_path: str, sort_by: str, run_date: str, target_table: str, schema_path: str, category_spec: str,
         bootstrap_servers: str= 'kafka-cp-kafka-headless.kafka:9092', kafka_topic: str = 'spyder_shopee_mall_item', retry_count=0):
    try:
        from kafka import KafkaProducer
        producer = KafkaProducer(
        bootstrap_servers=[bootstrap_servers],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
    except:
        producer = None

    base_dir = os.getcwd() + f'/{base_path}/{run_date}'
    pathlib.Path(base_dir).mkdir(parents=True, exist_ok=True)

    print(f"the start value is {start}")
    print(f"the retry count value is {retry_count}")
    if start == 0 and retry_count == 0:
        categories    = get_categories()[1]
        df_category   = get_categories()[0]
        name_category = get_categories()[2]
        random.shuffle(name_category)

        print("we use normal list category")
    else: 
        name_category = [category_spec]
        print("we dont use normal list category")

    max_retry = 3
    driver = set_webdriver()
    print(name_category)
    

    for i in range(len(name_category)):
        category = name_category[i]

        message = {'marketplace': 'shopee', 'job_type': 'extract', 'page_type': 'shopee_mall_item',
        'start_page': start, 'end_page': end, 'bucket_name': bucket_name, 'base_path': base_path, 
        'run_date': run_date, 'target_table': target_table, 'schema_path': schema_path, 'category_spec': name_category, 
        'retry_count': retry_count}

        for page in range(start, end):
            if message["start_page"] != page or len(name_category) > 1:
                retry_count = 0
            print(f"Doing page number {page}")
            
            try:
                extract_data(driver, page, category, df_category, base_dir, bucket_name, base_path, sort_by, run_date)
                sleep_condition(1,2)
                transform_data(category, page, bucket_name, base_path, target_table, schema_path, sort_by)
            except Exception as e:
                if producer is None:
                    logging.error(e)
                    continue
                else:
                    if retry_count <= max_retry:
                        retry_count = retry_count + 1
                        message["start_page"] = page
                        message["end_page"] = page + 1
                        message["retry_count"] = retry_count
                        producer.send(kafka_topic, value=message)
                        print(f'Producer send {kafka_topic}')
                        continue
                    else:
                        print("Max Retry Exceeded")
                        break
            sleep_condition(2,4)

    driver.quit()
