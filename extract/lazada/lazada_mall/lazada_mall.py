import os
import re
import json
import pathlib
import requests
import time
import logging
import random
import pandas as pd
from helpers.cloud_storage_helper import upload_blob_to_gcs, download_blob_to_local, list_blob_gcs
from helpers.webdriver_helper import connect_to_vpn, connect_to_oxylab, get_random_user_agent
from helpers.bigquery_helper import df_to_bq
from helpers.time_helper import sleep_condition
from seleniumwire import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from seleniumwire.utils import decode


def dict_to_gcs(data: dict, bucket_name: str, filename: str, blob_name: str):
    with open(filename, "w") as file:
        json.dump(data, file, indent=4)
    upload_blob_to_gcs(bucket_name=bucket_name, local_file_name=filename, gcs_blob_name=blob_name)
    print("successfully upload to gcs")
    return

def pandas_csv(filename):
    df = pd.read_csv(filename)
    return df

def parsing_json(data: dict):
    new_df = pd.json_normalize(data)
    column_names = list(new_df.columns.values)
    column_names = [text.replace('__', '').replace('.', '__').lower() for text in column_names]
    new_df.columns = column_names
    return new_df


def add_missing_columns(df, schema_path, timestamp):
    # Iterate over JSON schema files in the directory
    with open(schema_path, "r") as file:
        schema = json.load(file)

        # Convert the JSON schema to a list of column names
        columns_in_schema = [column["name"] for column in schema]

        # Get the list of columns in the DataFrame
        columns_in_df = list(df.columns)

        # Find columns in the DataFrame that are not in the schema
        extra_columns = set(columns_in_df) - set(columns_in_schema)

        # Remove extra columns from the DataFrame
        df.drop(extra_columns, axis=1, inplace=True)

        # Add missing columns to the DataFrame with None as the initial value
        missing_columns = set(columns_in_schema) - set(columns_in_df)
        for column in missing_columns:
            if column == "load_timestamp":
                df[column] = timestamp
            else:
                df[column] = None

    return df


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


def set_webdriver(proxy_ip=''):
    my_user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
    chrome_options = webdriver.ChromeOptions()
    # chrome_options.add_argument("--headless")
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


def extract_data(driver, page: int, category: str, base_dir: str, bucket_name: str, base_path: str, sort_by: str, run_date: str):
    driver.get(f"https://www.lazada.co.id/{shop_code}/?ajax=true&from=wangpu&isFirstRequest=true&langFlag=id&page={str(page)}&pageTypeId=2&q=All-Products")
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


def extract_with_driver(os_code_name: str, page: int, base_dir: str, base_path: str, run_date: str, bucket_name: str):
    body = None
    try:
        driver = set_webdriver()
        driver.get(f"https://www.lazada.co.id/{os_code_name}/?from=wangpu&langFlag=id&page={str(page)}&pageTypeId=2&q=All-Products")
        sleep_condition(10, 20)
        request_url = "?ajax=true"
        for request in driver.requests:
            if request.response:
                if request.url.find(request_url) > 0:
                    logging.info(request.url, request.response.status_code,
                                    request.response.headers['Content-Type'])
                    body = decode(request.response.body,
                                    request.response.headers.get('Content-Encoding', 'identity'))
        if body is not None:
            body = json.loads(body)
            all_items = body["mods"]["listItems"]
            filename = f"{base_dir}/{os_code_name}_{page}.json"
            blob_name = f"{base_path}/{run_date}/{os_code_name}_{page}.json"
            dict_to_gcs(all_items, bucket_name, filename, blob_name)
            print("finish add to gcs")
        else:
            print("body doesn't exist")
        
        driver.quit()

    except Exception as e:
        logging.error(e)
        print(f"Category {os_code_name} won't load")

    return all_items, blob_name




def get_api(shop_code: int, base_dir: str, page: int, bucket_name: str, base_path: str, run_date: str, oxylab_use: str): 
    url = f"https://www.lazada.co.id/{shop_code}/?ajax=true&from=wangpu&isFirstRequest=true&langFlag=id&page={str(page)}&pageTypeId=2&q=All-Products"
    print(url)

    if oxylab_use != "no":
        proxies = connect_to_oxylab()
        response = requests.get(url, headers=get_random_user_agent(), proxies=proxies)
    else: 
        response = requests.get(url, headers=get_random_user_agent())
        print("not using oxylab")


    try:
        time.sleep(3)
        json_data = response.json()
        print("json retrieved")

        # Convert the dictionary to a JSON-formatted string
        json_string = json.dumps(json_data)
        # Parse the JSON string into a Python dictionary
        data = json.loads(json_string)
        print("going into dataframe")
        print(data)

        # body = json.loads(body)
        all_items = data["mods"]["listItems"]
        filename = f"{base_dir}/{shop_code}_{str(page)}.json"
        blob_name = f"{base_path}/{run_date}/{shop_code}_{str(page)}.json"
        dict_to_gcs(all_items, bucket_name, filename, blob_name)
        return all_items, blob_name

    except:
        print("Failed to fetch JSON data:")
        pass


def main(start: int, end: int, bucket_name: str, base_path: str, run_date: str, timestamp,
         table_name: str, schema_path: str, oxylab_use="no", vpn_use="no"):
   
    # add path for saving json file
    base_dir = os.getcwd() + f'/{base_path}/{run_date}'
    pathlib.Path(base_dir).mkdir(parents=True, exist_ok=True)

    # read list category from csv file
    df_category = pandas_csv("./extract/lazada/lazada_mall/lazmall_list.csv")
    os_code = list(df_category['os_code'])
    random.shuffle(os_code)

    if vpn_use == "yes":
        vpn_filename = choose_vpn_filename()
        connect_to_vpn(vpn_filename)

    # loop by category
    for index, os_code_name in enumerate(os_code):
        print(f"processing {os_code_name}")

        pages = list(range(start, end))
        random.shuffle(pages)

        # loop by page
        for page in pages:
            print(f"processing page {page}")

            # do try mechanism to hit API with oxylab
            max_retries = 10
            retry = 0
            # body = None\
            while retry < max_retries:
                try:
                    all_items, blob_name = get_api(os_code_name, base_dir, page, 
                                                            bucket_name, base_path, run_date, oxylab_use)
                    

                    # all_items, breadcrumb, blob_name = extract_with_driver(os_code_name, page, base_dir,
                    #                                                         base_path, run_date, bucket_name)

                    # set variable for dataframe
                    df_final = pd.DataFrame()
                    break

                except Exception as e:
                    logging.error(e)
                    retry += 1
                    if retry < max_retries:
                        print(f"retry attempt {retry}/{max_retries} for {os_code_name} at page {page}")
                    else: 
                        print(f"OS {os_code_name} at page {page} won't load")
                    sleep_condition(5, 30)
                    continue


            # transform it so can be loaded into BQ
            for j in range(len(all_items)):
                item = all_items[j]
                try:
                    # category_order = json.dumps(breadcrumb)
                    df = parsing_json(item)
                    excluded_columns = ["skus", "icons", "categories", "thumbs"]
                    df = df.drop(excluded_columns, axis=1)
                    df = add_missing_columns(df, schema_path, timestamp)
                    df_final = pd.concat([df_final, df], ignore_index=True)
                except Exception as e:
                    logging.error(e)
                    print(f"Parsing Failed on {blob_name}")
                    continue
            try:
                df_to_bq(df_final, table_name, schema_path, "load_timestamp", "search_item")
            except Exception as e:
                logging.error(e)
                print("wrong schema for os code ", os_code_name, e)
                continue
            print(f"Data has been inserted to {table_name}")
            sleep_condition(5, 30)
        sleep_condition(5, 30)
